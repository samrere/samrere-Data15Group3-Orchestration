import logging
from typing import List, Dict, Optional, Union, Any
import io
from pydantic import BaseModel, HttpUrl
import pandas as pd
import fastavro
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_core.documents import Document
import pinecone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Job(BaseModel):
    """Job model"""
    job_id: str
    title: str
    company: str
    location: str
    employment_type: str
    seniority_level: str = ""
    industries: List[str]
    job_functions: List[str]
    workplace_type: str
    description: str
    skills: List[str]
    job_url: HttpUrl
    reposted: Optional[bool] = None
    posted_time: int
    expire_time: int
    apply_url: Optional[HttpUrl] = None


class JobDocumentLoader:
    """Custom document loader for Job objects"""

    def _sanitise_metadata_value(
            self, value: Any
    ) -> Union[str, float, bool, List[str]]:
        """Sanitise a single metadata value to Pinecone-compatible type"""
        if isinstance(value, (str, bool)):
            return value
        elif isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, list):
            return [str(item) for item in value]
        else:
            return str(value)

    def _create_metadata(
            self, job: Job
    ) -> Dict[str, Union[str, float, bool, List[str]]]:
        """Create Pinecone-compatible metadata dictionary from Job object"""
        try:
            raw_metadata = {
                "job_id": job.job_id,
                "title": job.title,
                "company": job.company,
                "location": job.location,
                "employment_type": job.employment_type,
                "seniority_level": job.seniority_level,
                "industries": job.industries,
                "job_functions": job.job_functions,
                "workplace_type": job.workplace_type,
                "skills": job.skills,
                "job_url": str(job.job_url),
                "reposted": bool(job.reposted) if job.reposted is not None else False,
                "posted_time": float(job.posted_time),
                "expire_time": float(job.expire_time),
                "apply_url": str(job.apply_url) if job.apply_url else None,
            }

            return {
                key: self._sanitise_metadata_value(value)
                for key, value in raw_metadata.items()
                if value is not None
            }
        except Exception as e:
            logger.error(f"Error creating metadata for job: {e}")
            logger.debug(f"Job data: {job}")
            raise ValueError(f"Failed to create metadata: {e}")

    def load_jobs(self, jobs_data: List[Dict]) -> List[Document]:
        """Convert job dictionaries to Documents with IDs"""
        try:
            documents = []
            for job_data in jobs_data:
                job = Job(**job_data)
                # Create document with job_id in metadata
                doc = Document(
                    page_content=job.description,
                    metadata={
                        **self._create_metadata(job),
                        "id": job.job_id,  # Ensure job_id is in metadata
                    },
                )
                documents.append(doc)
            logger.info(f"Converted {len(documents)} jobs to documents")
            return documents
        except Exception as e:
            logger.error(f"Error converting jobs to documents: {e}")
            raise


class VectorStorePipeline:
    """Pipeline for managing vector store entries"""

    def __init__(
            self,
            s3_bucket: str,
            openai_api_key: str,
            pinecone_api_key: str,
            pinecone_index_name: str,
    ):
        self.s3_bucket = s3_bucket
        self.s3 = S3Hook(aws_conn_id=None)
        self.embeddings = OpenAIEmbeddings(
            api_key=openai_api_key, model="text-embedding-3-large"
        )
        self.pc = pinecone.Pinecone(api_key=pinecone_api_key)
        self.index_name = pinecone_index_name

    def _get_vectorstore(self) -> PineconeVectorStore:
        """Initialize connection to vector store"""
        try:
            index = self.pc.Index(self.index_name)
            return PineconeVectorStore(index=index, embedding=self.embeddings)
        except Exception as e:
            logger.error(f"Failed to initialize vector store: {e}")
            raise

    def _read_avro_from_s3(self, date) -> List[dict]:
        """Read avro file from S3"""
        try:
            year = date.strftime('%Y')
            month = date.strftime('%m')
            day = date.strftime('%d')
            file_path = f"raw/{year}/{month}/{day}/Data_Engineer/Data_Engineer-{date.strftime('%Y%m%d')}.avro"
            # Get the file from S3
            response = self.s3.get_key(key=file_path, bucket_name=self.s3_bucket)
            # Read avro content
            avro_bytes = io.BytesIO(response.get()["Body"].read())
            avro_reader = fastavro.reader(avro_bytes)
            return list(avro_reader)
        except Exception as e:
            logger.error(f"Error reading avro file for date {date}: {e}")
            raise

    def _read_csv_from_s3(self) -> [List[str], None]:
        """Read CSV file from S3 and extract IDs"""
        try:
            # Construct the S3 path
            file_path = f"raw/invalid_jobs.csv"

            # Get the file from S3
            response = self.s3.get_key(key=file_path, bucket_name=self.s3_bucket)

            # Read CSV content
            df = pd.read_csv(io.BytesIO(response.get()["Body"].read()))
            return df["job_id"].astype(str).tolist()
        except Exception as e:
            print(f"Error reading invalid_jobs.csv file: {e}")
            return None

    def add_entries(self, current_date, batch_size: int = 100):
        """Add new job entries to vector store with job IDs"""
        try:
            logger.info(f"Starting job entry addition for date: {current_date}")
            # Read avro data
            jobs_data = self._read_avro_from_s3(current_date)
            logger.info(f"Read {len(jobs_data)} jobs from avro file")
            # Convert to documents
            loader = JobDocumentLoader()
            documents = loader.load_jobs(jobs_data)
            # Initialize vector store
            vectorstore = self._get_vectorstore()
            # Add documents in batches with IDs
            for i in range(0, len(documents), batch_size):
                batch = documents[i: i + batch_size]
                # Create ids list from job_ids in metadata
                ids = [doc.metadata["id"] for doc in batch]
                vectorstore.add_documents(documents=batch, ids=ids)
                logger.info(
                    f"Added batch {i // batch_size + 1}, "
                    f"processed {min(i + batch_size, len(documents))}/{len(documents)} documents"
                )
            logger.info(f"Successfully added {len(documents)} job entries")
            return f"Successfully added {len(documents)} job entries"
        except Exception as e:
            logger.error(f"Failed to add entries: {e}")
            raise

    def delete_entries(self):
        """Delete job entries from vector store"""
        try:
            logger.info(f"Starting job entry deletion")
            # Read IDs to delete
            ids_to_delete = self._read_csv_from_s3()

            if not ids_to_delete:
                logger.info("No IDs to delete")
                return "No IDs to delete"
            else:
                logger.info(f"Found {len(ids_to_delete)} IDs to delete")
            # Initialize vector store
            vectorstore = self._get_vectorstore()
            # Delete entries
            vectorstore.delete(ids_to_delete)
            logger.info(f"Successfully deleted {len(ids_to_delete)} entries")
            return f"Successfully deleted {len(ids_to_delete)} entries"
        except Exception as e:
            logger.error(f"Failed to delete entries: {e}")
            raise


if __name__ == '__main__':
    pipeline = VectorStorePipeline(
        s3_bucket="data15group3-job-data-lake",
        openai_api_key="...",
        pinecone_api_key="...",
        pinecone_index_name="de15-jd",
    )
    index = pipeline.pc.Index(pipeline.index_name)
    response = index.describe_index_stats()
    print(response)

    # job = {'a':1}
    # print(job.get("industries", []) or None)
    # date = datetime.now(timezone.utc)
    # year = date.strftime('%Y')
    # month = date.strftime('%m')
    # day = date.strftime('%d')
    # print(year, month, day)
    # print(date.strftime('%Y%m%d'))
