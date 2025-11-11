from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import numpy as np
from dotenv import load_dotenv
from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
import traceback

def run_milvus_pipeline():
    """
    Chạy pipeline Milvus (local hoặc cloud) từ Airflow.
    """

    # --- Load .env ---
    env_path = "/opt/airflow/.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️ Warning: .env file not found at {env_path}")

    # --- Đọc biến môi trường ---
    MODE = os.getenv("MODE", "LOCAL").upper()
    print(f"🌍 Running in {MODE} mode")

    try:
        # --- Kết nối ---
        if MODE == "LOCAL":
            host = os.getenv("LOCAL_HOST", "127.0.0.1")
            port = os.getenv("LOCAL_PORT", "19530")
            print(f"Connecting to Milvus Local at {host}:{port} ...")
            connections.connect(alias="default", host=host, port=port)
        elif MODE == "CLOUD":
            uri = os.getenv("CLOUD_URI")
            token = os.getenv("CLOUD_TOKEN")
            print(f"Connecting to Milvus Cloud at {uri} ...")
            if not uri or not token:
                raise ValueError("❌ CLOUD_URI hoặc CLOUD_TOKEN chưa được thiết lập trong .env")
            connections.connect(alias="default", uri=uri, token=token)
        else:
            raise ValueError("❌ MODE phải là 'LOCAL' hoặc 'CLOUD'")

        print("✅ Connected successfully!")

        # --- Kiểm tra hoặc tạo collection ---
        collection_name = "semantic_search"
        if not utility.has_collection(collection_name):
            print(f"🆕 Creating collection '{collection_name}'...")
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=200),
                FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=1000),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=128),
            ]
            schema = CollectionSchema(fields, description="Demo Semantic Search Collection")
            collection = Collection(name=collection_name, schema=schema)
            print("✅ Collection created.")
        else:
            collection = Collection(collection_name)
            print(f"📦 Using existing collection '{collection_name}'")

        # --- Dữ liệu mẫu ---
        titles = [
            "AI Assistant",
            "Vector Database",
            "Neural Network",
            "Data Science",
            "Cloud Computing"
        ]
        descriptions = [
            "An AI assistant that helps you with coding and text generation.",
            "A database optimized for storing and searching vector embeddings.",
            "A computational model inspired by the human brain.",
            "The field that combines statistics and machine learning to extract insights.",
            "Delivering computing services over the internet."
        ]
        embeddings = np.random.rand(5, 128).astype("float32")

        print("📥 Inserting data...")
        collection.insert([titles, descriptions, embeddings])
        collection.flush()
        print(f"✅ Inserted {len(titles)} documents and flushed to storage.")

        # --- Index + Load ---
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 64}
        }
        print("⚙️ Creating index and loading collection...")
        collection.create_index("embedding", index_params)
        collection.load()

        # --- Truy vấn thử ---
        query_vector = np.random.rand(1, 128).astype("float32")
        results = collection.search(
            data=query_vector,
            anns_field="embedding",
            param={"nprobe": 10},
            limit=3,
            output_fields=["title", "description"]
        )

        print("\n🔍 Semantic Search Results:")
        for hit in results[0]:
            print(f" - 🧩 {hit.entity.get('title')} (distance={hit.distance:.4f})")
            print(f"   ↳ {hit.entity.get('description')}\n")

        print(f"📊 Total entities in collection: {collection.num_entities}")

    except Exception as e:
        print("❌ ERROR OCCURRED:")
        traceback.print_exc()
        raise e  # Để Airflow đánh dấu task failed

# --- DAG định nghĩa ---
with DAG(
    dag_id="milvus_semantic_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["milvus", "semantic-search"]
) as dag:
    milvus_task = PythonOperator(
        task_id="run_milvus_pipeline",
        python_callable=run_milvus_pipeline,
    )
