import polars as pl
from validation import validate_batch

test_data= pl.DataFrame({
    "device_id": ["sensor-01", "sensor-02", None, "sensor-04", "   "],
    "temperature": [22.5, 300.0, 21.0, 19.5, 25.0],
    "humidity": [45.0, 50.0, 40.0, None, 60.0],
    "cpu_usage": [30.0, 50.0, -10.0, 20.0, 150.0],
})
result = validate_batch(test_data)
print(result)