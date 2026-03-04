import pandas as pd

def clean_column_names(df):
    """Standardize column names to lowercase, replace spaces, and rename 'st'."""
    df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
    df.rename(columns={'st': 'state'}, inplace=True)
    return df

def handle_invalid_values(df):
    """Standardize categorical values and clean percentage strings."""
    # Gender
    df['gender'] = df['gender'].replace({"Femal": "F", "female": "F", "Male": "M"})
    # State
    df['state'] = df['state'].replace({"AZ": "Arizona", "Cali": "California", "WA": "Washington"})
    # Education
    df['education'] = df['education'].replace("Bachelors", "Bachelor")
    # Vehicle Class
    vehicle_map = {"Sports Car": "Luxury", "Luxury SUV": "Luxury", "Luxury Car": "Luxury"}
    df['vehicle_class'] = df['vehicle_class'].replace(vehicle_map)
    # CLV
    if df['customer_lifetime_value'].dtype == 'O':
        df['customer_lifetime_value'] = df['customer_lifetime_value'].str.replace('%', '')
    df['customer_lifetime_value'] = pd.to_numeric(df['customer_lifetime_value'], errors='coerce')
    return df

def clean_complaints(df):
    """Extract the middle number from the complaint string format."""
    def extract_middle(val):
        if isinstance(val, str) and '/' in val:
            return val.split('/')[1]
        return val
    
    df['number_of_open_complaints'] = df['number_of_open_complaints'].apply(extract_middle)
    df['number_of_open_complaints'] = pd.to_numeric(df['number_of_open_complaints'], errors='coerce')
    return df

def handle_nulls_and_duplicates(df):
    """Fill missing values, drop duplicates, and fix types."""
    # Fill Categorical
    for col in ['gender', 'state', 'education', 'vehicle_class']:
        df[col] = df[col].fillna(df[col].mode()[0])
    
    # Fill Numerical
    num_cols = df.select_dtypes(include=['number']).columns
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median()).astype(int)
        
    # Duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    return df

def run_full_pipeline(df):
    """The 'Main' function to run everything in sequence."""
    df = clean_column_names(df)
    df = handle_invalid_values(df)
    df = clean_complaints(df)
    df = handle_nulls_and_duplicates(df)
    return df