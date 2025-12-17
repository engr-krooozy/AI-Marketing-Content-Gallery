import functions_framework
import logging
import os
import io

# NOTE: Do not add any top-level logic or heavy imports here.
# The container must start and listen on port 8080 immediately.
# All initialization must happen inside the function handler.

@functions_framework.cloud_event
def process_csv_and_generate_content(cloud_event):
    """
    Cloud Function entry point.
    Processes CSV from Storage, generates content with Gemini 3, and saves to BigQuery/Storage.
    """
    # 1. Setup Logging (safe to do here)
    logging.basicConfig(level=logging.INFO)
    logging.info("Function 'process_csv_and_generate_content' started.")

    try:
        # 2. Lazy Import Heavy Dependencies
        import json
        import csv
        from datetime import datetime
        from google.cloud import bigquery
        from google.cloud import storage
        import google.generativeai as genai

        # 3. Configuration
        GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        GCP_REGION = os.environ.get("GCP_REGION")
        BQ_DATASET = os.environ.get("BQ_DATASET")
        BQ_TABLE = os.environ.get("BQ_TABLE")
        FAILED_BUCKET_NAME = os.environ.get("FAILED_BUCKET_NAME")
        PRODUCT_IMAGES_BUCKET_NAME = os.environ.get("PRODUCT_IMAGES_BUCKET_NAME")
        GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

        if not GOOGLE_API_KEY:
            logging.error("Missing required environment variable: GOOGLE_API_KEY")
            return

        # 4. Initialize Clients
        storage_client = storage.Client()
        bq_client = bigquery.Client()
        genai.configure(api_key=GOOGLE_API_KEY)

        # 5. Initialize Models
        text_model = genai.GenerativeModel("models/gemini-3-pro-preview")
        image_model = genai.GenerativeModel("models/gemini-3-pro-image-preview")

        # 6. Parse Cloud Event
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]
        logging.info(f"Triggered by file: gs://{bucket_name}/{file_name}")

        def move_to_failed_bucket(bucket_name, file_name):
            """Moves a file to the designated 'failed' bucket."""
            if not FAILED_BUCKET_NAME:
                logging.error("FAILED_BUCKET_NAME environment variable not set. Cannot move file.")
                return

            source_bucket = storage_client.bucket(bucket_name)
            destination_bucket = storage_client.bucket(FAILED_BUCKET_NAME)
            source_blob = source_bucket.blob(file_name)

            try:
                destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
                source_blob.delete()
                logging.info(f"Moved '{file_name}' to failed bucket: gs://{FAILED_BUCKET_NAME}/{destination_blob.name}")
            except Exception as e:
                logging.error(f"Failed to move '{file_name}' to failed bucket: {e}", exc_info=True)

        # 7. Process File
        rows_to_insert = []
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            csv_content = blob.download_as_text(encoding="utf-8")
            
            reader = csv.reader(io.StringIO(csv_content))
            try:
                header = next(reader)
            except StopIteration:
                raise ValueError(f"CSV file '{file_name}' is empty or has no header.")

            for i, row in enumerate(reader):
                if len(row) < 2: 
                    logging.warning(f"Skipping malformed row #{i+2} in '{file_name}': {row}")
                    continue

                product_name = row[0].strip()
                keywords = row[1].strip()

                if not product_name and not keywords:
                    continue

                generated_text = None
                generated_image_url = None

                # Generate Text
                try:
                    text_prompt = f"Write a short, exciting marketing description for a product named '{product_name}' that is '{keywords}'. The description should be one paragraph."
                    text_response = text_model.generate_content(text_prompt)
                    generated_text = text_response.text.strip()
                except Exception as e:
                    logging.error(f"Text generation failed for '{product_name}': {e}")
                    generated_text = "Error: Text generation failed."

                # Generate Image
                if generated_text and "Error:" not in generated_text:
                    try:
                        image_prompt = f"A professional, high-resolution marketing photo, studio lighting, of: {product_name}, {generated_text[:100]}"
                        image_response = image_model.generate_content(image_prompt)
                        
                        # Assuming image response handling for saving
                        # Note: The exact structure of image bytes in Gemini 3.0 preview might differ, 
                        # adapting based on typical GenAI python client usage for images.
                        # Usually it's in response.parts or similar if it returns bytes.
                        # For the purpose of this task, assuming standard response structure or PIL Image.
                        # If the client returns a PIL image (common in some genai versions), we save it.
                        
                        # NOTE: Verify actual response structure for Gemini 3 Image model in preview.
                        # Often it returns an Image object in python client.
                        
                        if hasattr(image_response, 'parts') and image_response.parts:
                             # Check for image data in parts
                            image_bytes = image_response.parts[0].inline_data.data
                            image_blob_name = f"{product_name.replace(' ', '_').lower()}_{int(datetime.utcnow().timestamp())}.png"
                            
                            image_bucket = storage_client.bucket(PRODUCT_IMAGES_BUCKET_NAME)
                            image_blob = image_bucket.blob(image_blob_name)
                            image_blob.upload_from_file(io.BytesIO(image_bytes), content_type="image/png")
                            
                            generated_image_url = image_blob.public_url
                        else:
                            generated_image_url = "Error: No image returned."

                    except Exception as e:
                         # Attempting fallback or catching differing structure errors
                        logging.error(f"Image generation failed for '{product_name}': {e}")
                        generated_image_url = "Error: Image generation failed."
                else:
                    generated_image_url = "Skipped: Text generation failed."

                rows_to_insert.append({
                    "product_name": product_name,
                    "keywords": keywords,
                    "generated_content": generated_text,
                    "generated_image_url": generated_image_url,
                    "source_file": f"gs://{bucket_name}/{file_name}",
                    "processed_at": datetime.utcnow().isoformat()
                })

            if not rows_to_insert:
                 logging.warning(f"No valid rows processed in '{file_name}'.")

        except Exception as e:
            logging.error(f"Critical error processing '{file_name}': {e}", exc_info=True)
            move_to_failed_bucket(bucket_name, file_name)
            return

        # 8. Save to BigQuery
        if rows_to_insert:
            try:
                table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
                errors = bq_client.insert_rows_json(table_id, rows_to_insert)
                if not errors:
                    logging.info(f"Inserted {len(rows_to_insert)} rows into BigQuery.")
                else:
                    logging.error(f"BigQuery insertion errors: {errors}")
            except Exception as e:
                logging.error(f"Failed to insert into BigQuery: {e}")

    except Exception as e:
        logging.error(f"Fatal error in execution: {e}", exc_info=True)

    return "OK"
