# The Serverless AI Factory: From CSV to Content with Google Cloud

## Welcome to the Workshop!

Welcome! In this hands-on workshop, you'll build a powerful, resilient, multi-modal, serverless AI application on Google Cloud. Forget complex pipelines and long deployments. 

You will build an automated content factory that reads product information from a CSV file, uses Google's **Gemini 3.0 Pro** model to write compelling marketing descriptions, and then calls the **Gemini 3.0 Image** model to generate a unique product image to match. The entire process is protected by content moderation filters and includes robust error handling, with all results stored in **BigQuery**, Google's serverless data warehouse.

---

### What You'll Learn

*   How to set up a Google Cloud environment using Cloud Shell.
*   The fundamentals of a serverless, event-driven architecture.
*   How to write, configure, and deploy a production-ready Python **Cloud Function**.
*   How to call and chain multiple **Vertex AI** models (**Gemini** for text and **Gemini Image** for images).
*   How to implement content moderation using built-in safety filters.
*   How to build a resilient pipeline with robust error handling.
*   How to use **Cloud Storage** to trigger events and store multiple types of artifacts.
*   How to store and query structured results in **BigQuery**.
*   How to build and deploy a **Streamlit** frontend application on **Cloud Run**.

### Our Architecture

We will build a resilient, multi-modal system with several Google Cloud services. The primary workflow is:

<img width="796" height="725" alt="Screenshot 2025-12-21 at 05 56 22" src="https://github.com/user-attachments/assets/62ca1cc5-a48d-4959-b255-a034df7f429d" />

The Cloud Function then performs several actions:
1.  Calls **Vertex AI (Gemini)** to generate text.
2.  Calls **Gemini 3.0 Image** to generate an image from the text.
3.  Stores the generated text and image URL in **BigQuery**.
4.  Stores the generated image file in a separate **Cloud Storage (Image) Bucket**.

Finally, we have a **Streamlit App** running on **Cloud Run** that reads from BigQuery to display the results in a beautiful gallery.

If any file fails during processing, it is automatically moved to a **Cloud Storage (Failed) Bucket**.

---

## Section 1: Preparing Your Google Cloud Environment (Approx. 15 mins)

First, let's get your Google Cloud project and Cloud Shell ready.

> **Prerequisite:** You need a Google Cloud account with billing enabled.

### 1.1. Activate Cloud Shell

Cloud Shell is a browser-based command line with all the tools you need.

*   **Action:** In the Google Cloud Console, click the **Activate Cloud Shell** button (`>_`) in the top-right corner. A terminal pane will open at the bottom of your browser.
*   **Action:** In the Cloud Shell terminal, click the **Open Editor** button (it looks like a pencil) to open the code editor.

### 1.2. Configure Your Project and Region

Run the following commands in the Cloud Shell terminal to set up your environment.

```bash
# --- Configuration Commands ---

# 1. Set your Project ID. Replace "your-project-id" with your actual GCP Project ID.
gcloud config set project your-project-id
echo "Project configured."
```

```bash
# 2. Store your Project ID and Region in variables for easy use.
# You can change the region, but make sure it's one where all services are available.
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1" # A good default region
```

```bash

# 3. Confirm your settings.
echo "----------------------------------"
echo "Using Project ID: $PROJECT_ID"
echo "Using Region:     $REGION"
echo "----------------------------------"
```

### 1.3. Enable Required Google Cloud APIs

This command activates the APIs for the services we'll be using. It might take a minute or two.

```bash
# --- Enable APIs Command ---
echo "Enabling necessary GCP APIs... (This may take a few minutes)"
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  aiplatform.googleapis.com \
  iam.googleapis.com

echo "APIs enabled successfully."
```

### 1.4. Grant Permissions for Cloud Storage Triggers

To allow Cloud Storage to trigger our Cloud Function, we need to give its service account permission to publish events. This is a crucial step for event-driven functions.

```bash
# --- Grant Pub/Sub Publisher Role to GCS Service Account ---

# 1. Get the special Cloud Storage service account email address.
export GCS_SERVICE_ACCOUNT=$(gcloud storage service-agent --format 'get(email)')
```
```bash
# 2. Grant the Pub/Sub Publisher role to that service account.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GCS_SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher"

echo "Permissions granted to Cloud Storage service account."
```

---


### 1.5. Setup Google API Key

To access the Gemini 3.0 Pro models, we need a Google API Key.

1.  **Action:** Go to [Google AI Studio](https://aistudio.google.com/app/apikey) to get an API key.
2.  **Action:** In Cloud Shell, export this key as an environment variable.

```bash
# Export your Google API Key
export GOOGLE_API_KEY="your_api_key_goes_here"
```

---

## Section 2: Create Your Cloud Resources (Approx. 10 mins)

Next, let's create the storage bucket and BigQuery table that our application will use.

### 2.1. Create a Cloud Storage Bucket

Your bucket needs a globally unique name. We'll use your Project ID to help ensure it's unique.

```bash
# --- Create GCS Bucket ---
# The bucket will be used to upload the CSV files that trigger the process.
export BUCKET_NAME="ai-content-workshop-${PROJECT_ID}"
```

```bash
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME

echo "Created GCS Bucket: gs://$BUCKET_NAME"

# Create a bucket for files that cause processing errors
export FAILED_BUCKET_NAME="ai-content-workshop-failed-${PROJECT_ID}"
gsutil mb -p $PROJECT_ID -l $REGION gs://$FAILED_BUCKET_NAME

echo "Created GCS Bucket for failed files: gs://$FAILED_BUCKET_NAME"

# Create a bucket for the generated product images
export PRODUCT_IMAGES_BUCKET_NAME="ai-content-workshop-images-${PROJECT_ID}"
gsutil mb -p $PROJECT_ID -l $REGION gs://$PRODUCT_IMAGES_BUCKET_NAME

# Make the new image bucket public so the URLs work
gsutil iam ch allUsers:objectViewer gs://$PRODUCT_IMAGES_BUCKET_NAME

echo "Created public GCS Bucket for product images: gs://$PRODUCT_IMAGES_BUCKET_NAME"
# You can view your buckets here: https://console.cloud.google.com/storage/browser
```

### 2.2. Create a BigQuery Dataset and Table

Now, create a home for our data in BigQuery.

```bash
# --- Create BigQuery Dataset and Table ---
export BQ_DATASET="ai_workshop_dataset"
export BQ_TABLE="generated_content"
```
```bash
# Create the dataset
bq --location=$REGION mk --dataset \
    --description="Dataset for the AI Content Generator Workshop" \
    ${PROJECT_ID}:${BQ_DATASET}
```
```bash
# Create the table with a defined schema, now including a column for the image URL
bq mk --table \
    --description="Stores product info, AI-generated marketing content, and image URLs" \
    ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
    product_name:STRING,keywords:STRING,generated_content:STRING,generated_image_url:STRING,source_file:STRING,processed_at:TIMESTAMP

echo "Created BigQuery Dataset '${BQ_DATASET}' and Table '${BQ_TABLE}'"
# You can view your table here: https://console.cloud.google.com/bigquery
```

---

## Section 3: Create the Cloud Function (Approx. 15 mins)

This is the core of our workshop. We'll write the Python code that does all the work and tell Google Cloud what libraries it needs.

### 3.1. Create the Function's Source Code

Instead of manually creating files using a text editor, you can run the following commands in your Cloud Shell terminal. This will create the directory and the necessary source files (`main.py` and `requirements.txt`) for you.

**Action:** Run the following commands to create the function directory and the `main.py` file.

```bash
# Create the directory for the function's code
mkdir -p content-generator-function
```
```bash
# Create the main.py file using a "here document"
# Create the main.py file using a "here document"
cat > content-generator-function/main.py << EOF
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
                        
                        if hasattr(image_response, 'parts') and image_response.parts:
                            image_bytes = image_response.parts[0].inline_data.data
                            image_blob_name = f"{product_name.replace(' ', '_').lower()}_{int(datetime.utcnow().timestamp())}.png"
                            
                            image_bucket = storage_client.bucket(PRODUCT_IMAGES_BUCKET_NAME)
                            image_blob = image_bucket.blob(image_blob_name)
                            # Use io.BytesIO to upload from memory
                            image_blob.upload_from_file(io.BytesIO(image_bytes), content_type="image/png")
                            
                            generated_image_url = image_blob.public_url
                        else:
                            generated_image_url = "Error: No image returned."

                    except Exception as e:
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
EOF

```

**Action:** Run the following command to create the `requirements.txt` file.

```bash
# Create the requirements.txt file
# Create the requirements.txt file
cat > content-generator-function/requirements.txt << EOF
# This file lists the Python packages required by the Cloud Function.
# The Google Cloud Functions environment will automatically install these
# dependencies when the function is deployed.
functions-framework>=3.0.0
google-cloud-bigquery>=3.10.0
google-cloud-storage>=2.10.0
google-generativeai>=0.3.0
python-dotenv>=1.0.0
EOF

echo "Cloud Function source files created successfully."
```

### 3.3. Deploy the Cloud Function

Now, run this command in the Cloud Shell terminal to deploy your function. This step will take a few minutes.

```bash
# --- Deploy Cloud Function ---
# First, navigate into your function's directory
cd content-generator-function
```
```bash
# Now, deploy the function
gcloud functions deploy generate-content-from-csv \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=process_csv_and_generate_content \
  --trigger-bucket=$BUCKET_NAME \
  --trigger-bucket=$BUCKET_NAME \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,GCP_REGION=$REGION,BQ_DATASET=$BQ_DATASET,BQ_TABLE=$BQ_TABLE,FAILED_BUCKET_NAME=$FAILED_BUCKET_NAME,PRODUCT_IMAGES_BUCKET_NAME=$PRODUCT_IMAGES_BUCKET_NAME,GOOGLE_API_KEY=$GOOGLE_API_KEY \
  --memory=2Gi \
  --memory=2Gi \
  --timeout=540s


echo "Cloud Function deployment initiated."
# Return to the parent directory
cd ..
```

---

## Section 4: Test Your AI Content Generator (Approx. 10 mins)

Let's see our creation in action!

### 4.1. Create a Sample CSV File

*   **Action:** In the Cloud Shell Editor, create a new file in your root directory named `products.csv`.
*   **Action:** Copy and paste the following sample data into `products.csv`. Feel free to add your own products!

```csv
product_name,keywords
"Idanre Climber Boots","rainy season ready, waterproof leather, deep treads for slippery rocks, mud-resistant, for climbing Idanre hills and Olumo rock"
"Bodija Grind Master","grinds dry pepper and beans for akara, stainless steel body, transparent lid, durable sharp blades, market-grade"
"Efon Guard Zapper","mosquito killer, modern white lantern design, insect trap, child-safe cage, low-power UV light, no chemicals"
"Abike Auto-Gele","vibrant aso-oke print, pre-tied auto-gele style, silky fabric, bold yellow and blue geometric pattern, for Owambe parties"
"Iyalode Dinner Set","chip-resistant ceramic plates, traditional adire-inspired patterns, deep indigo and white glaze, service for four"
"Omorogun Pro","modern kitchen utensil, heat-resistant blue silicone spatula, smooth ergonomic wooden handle, lump-free Amala turning"
"Otunba Signature Wear","men's traditional kaftan, crisp cream-colored linen fabric, intricate gold embroidery on the chest, tailored fit for chiefs"
"Zenith Sleep Mask","100% blackout, contoured memory foam, zero eye pressure, adjustable strap, deep REM sleep, travel essential"
"Nomad Press Tumbler","portable french press, vacuum insulated, keeps coffee hot 8 hours, leak-proof, stainless steel, commuter friendly"
"HydroCore Smart Bottle","hydration tracker, glows to remind you to drink, self-cleaning UV light, keeps water cold 24hrs, bpa-free"
"GridLock Tech Pouch","cable management, waterproof origami design, accessories organizer, tangle-free storage, fits chargers and drives"
"CloudWalk Weighted Blanket","anxiety relief, cooling bamboo fabric, glass bead filling, deep pressure stimulation, machine washable, stress-free sleep"
"ChefMate 5-in-1 Slicer","vegetable chopper, mandoline slicer, safety hand guard, meal prep speed, stainless steel blades, easy clean"
"ErgoLift Laptop Stand","adjustable height, aluminum alloy, prevents neck strain, heat dissipation, portable folding design, remote work setup"
"LumaFlex Neck Light","hands-free reading, 3 brightness levels, eye-protection LED, rechargeable, partner-friendly night light, knitting and repair"
"SilentStep Yoga Mat","non-slip grip, extra thick cushioning, eco-friendly TPE, alignment lines, joint protection, sweat resistant"
"PureAir Desk Purifier","HEPA filter, removes allergens and dust, usb powered, whisper quiet, compact for office or bedside, fresh air"
"VoltKey Universal Adapter","all-in-one travel plug, 4 USB ports, works in 150+ countries, surge protection, fast charging, compact cube"
"GlowRing Selfie Light","clip-on ring light, 3 color modes, rechargeable battery, perfect for zoom calls and tiktok, pocket-sized studio lighting"
```

### 4.2. Upload the File to Cloud Storage

This is the trigger for our whole process.

```bash
# --- Upload CSV to Trigger the Function ---
gsutil cp products.csv gs://$BUCKET_NAME/

echo "Uploaded products.csv to gs://$BUCKET_NAME to trigger the function."
```

### 4.3. Verify the Results in BigQuery

Wait about a minute for the function to trigger and process the file. Then, run this query in the Cloud Shell to see the results.

```bash
# --- Query BigQuery to See Results ---
bq query --project_id=$PROJECT_ID "SELECT product_name, generated_content FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` ORDER BY processed_at DESC LIMIT 10"
```

You should see a table with your product names and the unique marketing descriptions generated by Gemini!

---

## Section 5: Implemented Feature: Content Moderation

In any application that generates content, it's crucial to ensure the output is safe and appropriate. The Vertex AI Gemini API includes built-in safety filters to help you moderate content. The Cloud Function has been automatically updated to enable these filters, preventing the generation of harmful text.

When Gemini blocks content due to a safety policy, the function will now log a warning and write a placeholder message, "Content blocked by safety filter," to BigQuery.

The `main.py` file created in Section 3 now includes this moderation logic. The key changes were:
1.  **Importing `HarmCategory` and `HarmBlockThreshold`** to define the safety rules.
2.  **Initializing the `GenerativeModel` with `safety_settings`**, which instructs the model to block content that reaches a medium or high probability of being harmful across several categories.
3.  **Checking `response.candidates`**: If this list is empty after a generation call, it confirms the content was blocked, and the function handles it gracefully.

You can now proceed to the next section to test the full pipeline, now with enhanced safety features.

---

## Section 6: Implemented Feature: Robust Error Handling

To make our pipeline more resilient, the Cloud Function has been updated to handle processing failures gracefully. If a file is uploaded that is empty, malformed, or causes any other critical error during processing, the function will now automatically move it to a separate "failed files" bucket for you to inspect later.

This prevents a single bad file from blocking the entire content generation process.

The key changes were:
1.  **Creating a `FAILED_BUCKET_NAME`**: A second Cloud Storage bucket is now created to isolate problematic files.
2.  **Passing the Bucket Name as an Environment Variable**: The deploy command was updated to pass the new bucket's name to the function.
3.  **Implementing `move_to_failed_bucket`**: A helper function was added to handle the logic of moving a file from the main bucket to the failed bucket.
4.  **Adding a Global `try...except` Block**: The core file processing logic is now wrapped in an error handler. If any exception occurs, it's caught, the error is logged, and the file is moved, ensuring the function exits cleanly.

---

## Section 7: Implemented Feature: AI-Powered Image Generation

The pipeline is now a truly multi-modal content factory. In addition to generating marketing text, the Cloud Function now calls a second powerful Vertex AI model, **Gemini 3.0 Image**, to create a unique product image based on the text description it just wrote.

This showcases the power of chaining different AI models together to build sophisticated, automated workflows.

The key changes to implement this were:
1.  **Creating a Public Image Bucket**: A new, publicly-accessible Cloud Storage bucket is created to store the generated images so their URLs can be shared.
2.  **Adding an Image URL Column to BigQuery**: The BigQuery table schema was altered to include a `generated_image_url` column.
3.  **Initializing the `ImageGenerationModel`**: The function now initializes the `imagegeneration@006` model in addition to the Gemini text model.
4.  **Implementing the Image Generation Flow**:
    *   After text is generated, a new prompt is created for the image model.
    *   The model generates the image, which is returned as raw bytes.
    *   The function uploads these bytes to the public image bucket, creating a `.png` file.
    *   The public URL of this new image is saved to the `generated_image_url` column in BigQuery.

---
---

## Section 9: Bonus - Build a Marketing Gallery App

Let's not keep these amazing results hidden in database tables. We will build a web app to showcase your products!

### 9.1 Create the App Files

Run these commands to create a simple Streamlit app.

```bash
# Create the frontend app directory
mkdir -p frontend-app

# Create app.py
cat > frontend-app/app.py << EOF
import streamlit as st
from google.cloud import bigquery
import os

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BQ_DATASET")
TABLE_ID = os.environ.get("BQ_TABLE")

st.set_page_config(page_title="AI Marketing Gallery", layout="wide")

st.title("🛍️ AI Marketing Content Gallery")
st.markdown(f"Displaying results from BigQuery table: \`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}\`")

if not PROJECT_ID or not DATASET_ID or not TABLE_ID:
    st.error("Missing environment variables. Please check your deployment.")
else:
    try:
        client = bigquery.Client()
        
        # Query the latest 20 results
        query = f"""
            SELECT product_name, keywords, generated_content, generated_image_url
            FROM \`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}\`
            WHERE generated_image_url IS NOT NULL 
              AND generated_image_url NOT LIKE 'Error%'
            ORDER BY processed_at DESC
            LIMIT 20
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        cols = st.columns(3) # Create a grid layout
        
        for i, row in enumerate(results):
            with cols[i % 3]:
                st.subheader(row.product_name)
                if row.generated_image_url:
                    st.image(row.generated_image_url, use_container_width=True)
                
                st.markdown("**Keywords:** " + row.keywords)
                with st.expander("Read Marketing Copy"):
                    st.write(row.generated_content)
                st.divider()
                
    except Exception as e:
        st.error(f"Error connecting to BigQuery: {e}")
EOF
```

```bash
# Create requirements.txt
cat > frontend-app/requirements.txt << EOF
streamlit
google-cloud-bigquery
pandas
db-dtypes
EOF
```

```bash
# Create Dockerfile
cat > frontend-app/Dockerfile << EOF
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
EOF
```

### 9.2 Deploy to Cloud Run

We will deploy this app container to Cloud Run, Google's serverless container platform.

```bash
# Build the container image using Cloud Build
cd frontend-app
gcloud builds submit --tag gcr.io/$PROJECT_ID/marketing-gallery
cd ..
```

```bash
# Deploy to Cloud Run
gcloud run deploy marketing-gallery \
  --image gcr.io/$PROJECT_ID/marketing-gallery \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,BQ_TABLE=$BQ_TABLE
```

Wait a moment, and you will get a **Service URL**. Click it to see your AI-generated product gallery!

---

## Section 10: Conclusion & Cleanup

Congratulations! You've successfully built and tested a serverless AI application on Google Cloud. You learned how to connect several powerful services to create an intelligent, automated workflow.

### 10.1. Cleanup

To avoid ongoing charges, run these commands to delete the resources you created.

```bash
# ---- Cleanup Commands ----

# 1. Delete the Cloud Function
gcloud functions delete generate-content-from-csv --region=$REGION --gen2 --quiet

# 2. Delete the Cloud Run Service
gcloud run services delete marketing-gallery --region=$REGION --quiet

# 3. Delete the GCS Bucket (the -r and -f flags remove all contents first)
gsutil rm -r -f gs://$BUCKET_NAME
gsutil rm -r -f gs://$PRODUCT_IMAGES_BUCKET_NAME
gsutil rm -r -f gs://$FAILED_BUCKET_NAME

# 4. Delete the BigQuery Dataset (the -r and -f flags remove all tables first)
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

echo "Cleanup complete. All resources have been deleted."
```
---
Google Cloud credits are provided for this project. #AISprint #AISprintH2 #AISprint2025

**Thank you for participating in the workshop!**
