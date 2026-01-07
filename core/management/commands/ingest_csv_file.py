from typing import Any

from django.core.management.base import BaseCommand

from core.services.ingestion import ingest_csv_data
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
            with open(csv_file_path, encoding='utf-8') as file_obj:
                success_count, failure_count = ingest_csv_data(file_obj, strategy)
        
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"File not found: {csv_file_path}"))
            return
        except OSError as os_err:
             self.stdout.write(self.style.ERROR(f"Error opening/reading file: {str(os_err)}"))
             return
        except Exception as unexpected_error:
             self.stdout.write(self.style.ERROR(f"Fatal error: {str(unexpected_error)}"))
             return

        self.stdout.write(self.style.SUCCESS(f"Ingestion complete. Success: {success_count}, Failed: {failure_count}"))


