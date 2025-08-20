package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "strconv"
    "time"

    "cloud.google.com/go/bigquery"
    "github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

// OpenMeteoResponse defines the structure for the Open-Meteo API response.
type OpenMeteoResponse struct {
    Latitude  float64   `json:"latitude"`
    Longitude float64   `json:"longitude"`
    Daily     DailyData `json:"daily"`
}

// DailyData defines the daily weather data arrays.
type DailyData struct {
    Time             []string  `json:"time"`
    Temperature2mMin []float64 `json:"temperature_2m_min"`
    Temperature2mMax []float64 `json:"temperature_2m_max"`
    Temperature2mMean []float64 `json:"temperature_2m_mean"`
    RainSum          []float64 `json:"rain_sum"`
    SnowfallSum      []float64 `json:"snowfall_sum"`
}

// WeatherData represents the schema for BigQuery.
type WeatherData struct {
    Latitude        float64   `bigquery:"latitude"`
    Longitude       float64   `bigquery:"longitude"`
    Date            string    `bigquery:"date"`
    MeanTemperature float64   `bigquery:"mean_temperature"`
    MinTemperature  float64   `bigquery:"min_temperature"`
    MaxTemperature  float64   `bigquery:"max_temperature"`
    RainSum         float64   `bigquery:"rain_sum"`
    SnowfallSum     float64   `bigquery:"snowfall_sum"`
    InsertedAt      time.Time `bigquery:"inserted_at"`
}

// init registers the HTTP function.
func init() {
    functions.HTTP("FetchWeatherData", fetchWeatherData)
}

// fetchWeatherData handles the HTTP request, fetches weather data, and stores it in BigQuery.
func fetchWeatherData(w http.ResponseWriter, r *http.Request) {
    ctx := context.Background()

    // Parse query parameters for latitude and longitude.
    latStr := r.URL.Query().Get("latitude")
    lonStr := r.URL.Query().Get("longitude")
    if latStr == "" || lonStr == "" {
        http.Error(w, "Missing latitude or longitude", http.StatusBadRequest)
        return
    }
    latitude, _ := strconv.ParseFloat(latStr, 64)
    longitude, _ := strconv.ParseFloat(lonStr, 64)

    // Define date range (last 20 years).
    endDate := time.Now().Format("2006-01-02")
    startDate := time.Now().AddDate(-20, 0, 0).Format("2006-01-02")

    // Fetch weather data from Open-Meteo.
    apiURL := fmt.Sprintf(
        "https://archive-api.open-meteo.com/v1/archive?latitude=%f&longitude=%f&start_date=%s&end_date=%s&daily=temperature_2m_min,temperature_2m_max,temperature_2m_mean,rain_sum,snowfall_sum&timezone=auto",
        latitude, longitude, startDate, endDate,
    )

    resp, err := http.Get(apiURL)
    if err != nil {
        log.Printf("Failed to make HTTP request: %v", err)
        http.Error(w, "Failed to fetch data", http.StatusInternalServerError)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        log.Printf("Open-Meteo API returned status %d: %s", resp.StatusCode, string(body))
        http.Error(w, "API error", http.StatusInternalServerError)
        return
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Printf("Failed to read response body: %v", err)
        http.Error(w, "Failed to read data", http.StatusInternalServerError)
        return
    }

    var meteoResp OpenMeteoResponse
    if err := json.Unmarshal(body, &meteoResp); err != nil {
        log.Printf("Failed to unmarshal JSON: %v", err)
        http.Error(w, "Failed to parse data", http.StatusInternalServerError)
        return
    }

    if len(meteoResp.Daily.Time) == 0 {
        log.Printf("No data returned from API")
        http.Error(w, "No data available", http.StatusNoContent)
        return
    }

    // Prepare data for BigQuery.
    var weatherData []*WeatherData
    for i := 0; i < len(meteoResp.Daily.Time); i++ {
        entry := &WeatherData{
            Latitude:        meteoResp.Latitude,
            Longitude:       meteoResp.Longitude,
            Date:            meteoResp.Daily.Time[i],
            MeanTemperature: meteoResp.Daily.Temperature2mMean[i],
            MinTemperature:  meteoResp.Daily.Temperature2mMin[i],
            MaxTemperature:  meteoResp.Daily.Temperature2mMax[i],
            RainSum:         meteoResp.Daily.RainSum[i],
            SnowfallSum:     meteoResp.Daily.SnowfallSum[i],
            InsertedAt:      time.Now(),
        }
        weatherData = append(weatherData, entry)
    }

    // Initialize BigQuery client.
    client, err := bigquery.NewClient(ctx, "dataform-intro-469416")
    if err != nil {
        log.Printf("Failed to create BigQuery client: %v", err)
        http.Error(w, "BigQuery error", http.StatusInternalServerError)
        return
    }
    defer client.Close()

    // Insert data into BigQuery.
    datasetID := "weather_dataset"
    tableID := "daily_weather"
    inserter := client.Dataset(datasetID).Table(tableID).Inserter()
    if err := inserter.Put(ctx, weatherData); err != nil {
        log.Printf("Failed to insert data: %v", err)
        http.Error(w, "Failed to store data", http.StatusInternalServerError)
        return
    }

    fmt.Fprintf(w, "Successfully inserted %d rows into BigQuery", len(weatherData))
}
