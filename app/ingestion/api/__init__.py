import io
from datetime import datetime

import polars as pl
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app.lake.write_delta import WriteMode, write_delta

app = FastAPI(
    title="CSV to Delta Lake Ingest API",
    description="API for uploading CSV files and converting to Delta Lake format",
    version="1.0.0",
)


def sanitize_table_name(name: str) -> str:
    """Convert table name to a safe format for storage"""
    return name.lower().replace(" ", "_").replace("-", "_")


@app.post("/ingest-csv/")
async def ingest_csv(
    file: UploadFile = File(...),
    table_name: str = "uploaded_data",
    overwrite: bool = True,
):
    """
    Upload a CSV file and convert it to Delta Lake format.

    Parameters:
    - file: CSV file to upload
    - table_name: Name for the Delta table (will be sanitized)
    - overwrite: Whether to overwrite existing table

    Returns:
    - JSON response with operation status
    """
    start_time = datetime.now()

    try:
        # Sanitize and validate table name
        sanitized_name = sanitize_table_name(table_name)
        if not sanitized_name.isidentifier():
            raise HTTPException(
                status_code=400,
                detail="Invalid table name - must be alphanumeric with underscores",
            )

        # Generate unique path in the data lake
        formatted_table_name = f"{sanitized_name}_{datetime.now().strftime('%Y%m%d')}"

        # Read CSV content
        contents = await file.read()

        # Use Polars to read CSV (similar to your existing code)
        df = pl.read_csv(io.BytesIO(contents))

        if df.is_empty():
            raise HTTPException(
                status_code=400,
                detail="Uploaded CSV file is empty or could not be parsed",
            )

        # Write to Delta format
        write_delta(
            df,
            formatted_table_name,
            write_mode=WriteMode.OVERWRITE if overwrite else WriteMode.ERROR,
        )

        elapsed = (datetime.now() - start_time).total_seconds()

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "File successfully ingested to Delta Lake",
                "table_name": formatted_table_name,
                "row_count": df.shape[0],
                "columns": df.columns,
                "processing_time_seconds": elapsed,
            },
        )

    except pl.ComputeError as e:
        raise HTTPException(status_code=400, detail=f"Data processing error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
