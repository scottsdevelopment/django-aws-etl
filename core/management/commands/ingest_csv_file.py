import os
from typing import Any

from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand

from core.models import Artifact
from core.services.processing_service import process_artifact
from core.services.raw_ingestion_service import ingest_file_to_raw
from core.strategies import get_strategy


class Command(BaseCommand):
    help = 'Ingests a CSV file into the database using a specified strategy (audit, pharmacy, etc.)'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file to ingest')
        parser.add_argument(
            '--type', 
            type=str, 
            default='audit', 
            help='Type of data to ingest (e.g., audit, pharmacy). Defaults to "audit".'
        )

    def handle(self, *args: 'Any', **options: 'Any'):
        csv_file_path = options['csv_file']
        data_type = options['type']
        
        strategy = get_strategy(data_type)
        if not strategy:
            self.stdout.write(self.style.ERROR(f"Unknown data type: {data_type}"))
            return

        self.stdout.write(f"Starting ingestion of {data_type} from {csv_file_path}...")

        try:
            # Check file exists first
            if not os.path.exists(csv_file_path):
                 self.stdout.write(self.style.ERROR(f"File not found: {csv_file_path}"))
                 return

            with open(csv_file_path, 'rb') as f:
                content = f.read()
                
            filename = os.path.basename(csv_file_path)
            file_obj = ContentFile(content, name=filename)
            
            # 1. Ingest to Raw
            artifact = ingest_file_to_raw(file_obj, filename, data_type)
            
            if artifact.status == Artifact.FAILED:
                self.stdout.write(self.style.ERROR("Raw ingestion failed. Check logs."))
                return

            # 2. Process Artifact/Raw rows
            success_count, failure_count = process_artifact(artifact.id)
        
        except OSError as os_err:
             self.stdout.write(self.style.ERROR(f"Error opening/reading file: {str(os_err)}"))
             return
        except Exception as unexpected_error:
             self.stdout.write(self.style.ERROR(f"Fatal error: {str(unexpected_error)}"))
             return

        self.stdout.write(self.style.SUCCESS(f"Ingestion complete. Success: {success_count}, Failed: {failure_count}"))
