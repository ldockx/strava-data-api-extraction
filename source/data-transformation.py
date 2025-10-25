import logging
from pathlib import Path
from typing import Tuple
import pandas as pd

# Paths
RAW_DATA_DIR = Path('data/raw data')
TRANSFORMED_DATA_DIR = Path('data/transformed data')

RAW_ACTIVITIES_FILE = RAW_DATA_DIR / 'activities_data.csv'
RAW_STREAM_FILE = RAW_DATA_DIR / 'coordinates_data.csv'

# Business Logic Constants
MARATHON_DATE = pd.Timestamp('2025-10-12', tz='UTC')
TRAINING_MONTHS = 4
SPORT_TYPE = 'Run'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# define user functions

def ensure_dir_exists(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured directory exists: {directory}")


def write_data_to_csv(df: pd.DataFrame, filename: str, output_dir: Path = TRANSFORMED_DATA_DIR) -> None:
    ensure_dir_exists(output_dir)
    file_path = output_dir / filename
    df.to_csv(file_path, index=False)
    logger.info(f"✅ Saved {len(df)} records to: {file_path}")

"""not sure if include this"""
def validate_dataframe(df: pd.DataFrame, required_columns: list, df_name: str) -> None:
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"{df_name} missing required columns: {missing_cols}")
    logger.debug(f"✅ Validated {df_name}")


# load data

def load_raw_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading raw data...")
    
    if not RAW_ACTIVITIES_FILE.exists():
        raise FileNotFoundError(f"Activities file not found: {RAW_ACTIVITIES_FILE}")
    if not RAW_STREAM_FILE.exists():
        raise FileNotFoundError(f"Stream file not found: {RAW_STREAM_FILE}")
    
    activities_data = pd.read_csv(RAW_ACTIVITIES_FILE)
    stream_data = pd.read_csv(RAW_STREAM_FILE)
    
    logger.info(f"Loaded {len(activities_data)} activities and {len(stream_data)} stream records")
    
    return activities_data, stream_data


# transform data

def prepare_activities_data(activities_data: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparing activities data...")
    
    # Validate required columns
    validate_dataframe(
        activities_data,
        ['start_date', 'sport_type', 'distance', 'id'],
        'activities_data'
    )
    
    # Create copy to avoid modifying original
    df = activities_data.copy()
    
    # Convert date column
    df['start_date'] = pd.to_datetime(df['start_date'])
    
    logger.info("✅ Activities data prepared")
    return df


def extract_marathon_data(
    activities_data: pd.DataFrame,
    stream_data: pd.DataFrame
) -> Tuple[pd.Series, pd.DataFrame]:
    logger.info("Extracting marathon data...")
    
    # Get longest run
    runs = activities_data[activities_data['sport_type'] == SPORT_TYPE]
    
    if runs.empty:
        raise ValueError(f"No activities found with sport_type='{SPORT_TYPE}'")
    
    marathon_activity = runs.sort_values('distance', ascending=False).iloc[0]
    
    # Get corresponding stream data
    marathon_stream = stream_data[stream_data['activity_id'] == marathon_activity['id']]
    
    logger.info(
        f"✅ Marathon: {marathon_activity['distance']:.2f}m on "
        f"{marathon_activity['start_date'].date()}"
    )
    
    return marathon_activity, marathon_stream


def extract_training_data(
    activities_data: pd.DataFrame,
    stream_data: pd.DataFrame,
    end_date: pd.Timestamp,
    months: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Extracting training data ({months} months before {end_date.date()})...")
    
    start_date = end_date - pd.DateOffset(months=months)
    
    # Filter training activities
    training_activities = activities_data[
        (activities_data['sport_type'] == SPORT_TYPE) &
        (activities_data['start_date'].between(start_date, end_date))
    ]
    
    # Get stream data for training activities
    training_stream = stream_data[
        stream_data['activity_id'].isin(training_activities['id'])
    ]
    
    logger.info(
        f"✅ Training period: {start_date.date()} to {end_date.date()} "
        f"({len(training_activities)} runs, {len(training_stream)} stream records)"
    )
    
    return training_activities, training_stream


# main

def run_etl_pipeline() -> None:
    try:
        logger.info("=" * 60)
        logger.info("Starting Marathon Data ETL Pipeline")
        logger.info("=" * 60)
        
        # Extract
        activities_data, stream_data = load_raw_data()
        
        # Transform
        activities_data = prepare_activities_data(activities_data)
        
        marathon_activity, marathon_stream = extract_marathon_data(
            activities_data, stream_data
        )
        
        training_activities, training_stream = extract_training_data(
            activities_data, stream_data, MARATHON_DATE, TRAINING_MONTHS
        )
        
        # Load (Write outputs)
        logger.info("Writing transformed data...")
        
        # Convert Series to DataFrame for marathon_activity
        marathon_activity_df = marathon_activity.to_frame().T
        
        write_data_to_csv(marathon_activity_df, "marathon_activity.csv")
        write_data_to_csv(marathon_stream, "marathon_stream.csv")
        write_data_to_csv(training_activities, "training_activities.csv")
        write_data_to_csv(training_stream, "training_stream.csv")
        
        logger.info("=" * 60)
        logger.info("✅ ETL Pipeline completed successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_etl_pipeline()