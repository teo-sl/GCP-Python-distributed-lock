from google.cloud import storage
from datetime import datetime, timedelta
import datetime as dt
import threading
import time
import json
import random
from google.api_core.exceptions import PreconditionFailed, NotFound
 
 
class DistributedLock:
    def __init__(self, object_url, ttl=300, refresh_interval=37, max_refresh_failures=3, identity=None):
        # add your project id if necessary
        self.client = storage.Client()
        self.bucket_name, self.object_name = self._parse_gcs_url(object_url)
        self.bucket = self.client.bucket(self.bucket_name)
        self.ttl = ttl
        self.refresh_interval = refresh_interval
        self.max_refresh_failures = max_refresh_failures
        self.identity = identity or f"process-{id(self)}"
        self.lock_thread = None
        self.stop_refreshing = threading.Event()
        self.metageneration = None
 
    def _parse_gcs_url(self, url):
        if not url.startswith("gs://"):
            raise ValueError("URL must start with 'gs://'")
        parts = url[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError("URL must be in the format 'gs://<bucket>/<object>'")
        return parts[0], parts[1]
 
    def _get_object_metadata(self):
        blob = self.bucket.get_blob(self.object_name)
        if blob:
            metadata = {
                "update_timestamp": blob.updated,
                "metageneration": blob.metageneration,
                "expiration_timestamp": blob.metadata.get("expiration_timestamp"),
                "identity": blob.metadata.get("identity")
            }
            return metadata
        return None
 
    def _create_lock_object(self):
        blob = self.bucket.blob(self.object_name)
        if blob.exists():
            return False
        expiration_timestamp = (datetime.now() + timedelta(seconds=self.ttl)).isoformat()
        metadata = {
            "expiration_timestamp": expiration_timestamp,
            "identity": self.identity
        }
        try:
            blob.metadata = metadata
            blob.cache_control = "no-store"
            blob.upload_from_string("", if_generation_match=0)
            return True
        except Exception as e:
            if isinstance(e, PreconditionFailed):
                return False
            raise
 
    def _delete_lock_object(self, metageneration=None):
        blob = self.bucket.blob(self.object_name)
        try:
            blob.delete(if_metageneration_match=metageneration)
        except PreconditionFailed:
            pass
 
    def _refresh_lock(self):
        consecutive_failures = 0
        while not self.stop_refreshing.is_set():
            time.sleep(self.refresh_interval)
            blob = self.bucket.blob(self.object_name)
            expiration_timestamp = (datetime.now(dt.timezone.utc) + timedelta(seconds=self.ttl)).isoformat()
            try:
                blob.metadata = {"expiration_timestamp": expiration_timestamp, "identity": self.identity}
                blob.patch(if_metageneration_match=self.metageneration)
                self.metageneration = blob.metageneration
                consecutive_failures = 0
            except NotFound:
                self.stop_refreshing.set()
                return
            except PreconditionFailed:
                self.stop_refreshing.set()
                return
            except Exception as e:
                consecutive_failures += 1
                if consecutive_failures >= self.max_refresh_failures:
                    self.stop_refreshing.set()
                    return
 
    def take_lock(self):
        while True:
            if self._create_lock_object():
                self.metageneration = self._get_object_metadata()["metageneration"]
                self.stop_refreshing.clear()
                self.lock_thread = threading.Thread(target=self._refresh_lock, daemon=True)
                self.lock_thread.start()
                return True
            metadata = self._get_object_metadata()
            if metadata:
                if metadata["identity"] == self.identity:
                    self._delete_lock_object(metadata["metageneration"])
                    continue
                expiration_timestamp = datetime.fromisoformat(metadata["expiration_timestamp"]).timestamp()
                if datetime.now(dt.timezone.utc).timestamp() > expiration_timestamp:
                    self._delete_lock_object(metadata["metageneration"])
                else:
                    time.sleep(random.uniform(0.1, 1.0))  # Exponential backoff with jitter
 
    def release_lock(self):
        self.stop_refreshing.set()
        if self.lock_thread:
            self.lock_thread.join()
        self._delete_lock_object(self.metageneration)
