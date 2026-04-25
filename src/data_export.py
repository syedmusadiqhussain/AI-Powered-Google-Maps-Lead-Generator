import os
import pandas as pd
from .utils import sanitize_filename_component

def save_places_to_excel(places_data, filename):
    """
    Save places data to an Excel file in the 'data' folder.
    
    Args:
        places_data (List[Dict]): List of places data from the Serper Maps API
        filename (str): Name of the Excel file to save
    """
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    safe_filename = sanitize_filename_component(filename)
    if not safe_filename.lower().endswith(".xlsx"):
        safe_filename = f"{safe_filename}.xlsx"

    # Set the full file path
    file_path = os.path.join(data_dir, safe_filename)
    
    # Extract places from all pages
    all_places = []
    for page_data in places_data:
        if 'places' in page_data:
            all_places.extend(page_data['places'])
    
    if not all_places:
        print("No places data to save.")
        return
    
    # Create DataFrame with relevant columns
    df = pd.DataFrame([{
        'name': place.get('title', ''),
        'address': place.get('address', ''),
        'website': place.get('website', '') or place.get('url', ''),
        'phone': place.get('phoneNumber', ''),
        'description': place.get('description', ''),
        'rating': place.get('rating', ''),
        'reviews': place.get('ratingCount', ''),
        'category': place.get('type', ''),
        'keywords': " || ".join(place.get('types', [])),
        'price_level': place.get('priceLevel', ''),
        'opening_hours': place.get('openingHours', {}),
        'email': '',
        'email_health': '',
        'facebook': '',
        'twitter': '',
        'instagram': '',
        'linkedin_url': '',
        'searched': 'NO',
    } for place in all_places])
    
    # Save to Excel
    df.to_excel(file_path, index=False)
    print(f"Data saved to {file_path}")
    return file_path

def update_business_data(df, index, info):
    """
    Helper function to update a business's information in the Excel DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing the businesses
        index: Row index to update
        info (Dict[str, Any]): Information to update (email, social media links)
    """
    # Update the row with the new information using the provided index
    if info:
        df.at[index, 'email'] = info.get('email', '')
        if 'email_health' in info:
            if 'email_health' not in df.columns:
                df['email_health'] = ""
            df.at[index, 'email_health'] = info.get('email_health', '')
        df.at[index, 'facebook'] = info.get('facebook', '')
        df.at[index, 'twitter'] = info.get('twitter', '')
        df.at[index, 'instagram'] = info.get('instagram', '')
        if 'linkedin_url' in info:
            if 'linkedin_url' not in df.columns:
                df['linkedin_url'] = ""
            df.at[index, 'linkedin_url'] = info.get('linkedin_url', '')
    df.at[index, 'searched'] = "YES"

def load_excel_data(filename: str) -> pd.DataFrame:
    """
    Load places data from an Excel file.
    
    Args:
        filename (str): Name of the Excel file to load
        
    Returns:
        pd.DataFrame: DataFrame containing the places data
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data")

    candidates = []
    if filename:
        candidates.append(filename)
        candidates.append(os.path.join(project_root, filename))
        candidates.append(os.path.join(data_dir, filename))

    file_path = next((p for p in candidates if os.path.exists(p)), None)
    if not file_path:
        raise FileNotFoundError(f"Data file not found: {filename}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    # Load DataFrame from Excel
    df = pd.read_excel(file_path)
    
    # Replace Nan with ""
    df = df.fillna("")
    
    return df, file_path
