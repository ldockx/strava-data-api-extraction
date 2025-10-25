import pandas as pd
import folium
#from folium import plugins
#import numpy as np
import ast

# load data

marathon_stream = pd.read_csv('data/transformed data/marathon_stream.csv')

marathon_stream['latlng'] = marathon_stream['latlng'].apply(ast.literal_eval)
marathon_stream['latitude'] = marathon_stream['latlng'].str[0]
marathon_stream['longitude'] = marathon_stream['latlng'].str[1]

# prepare data

def normalize_values(values):
    """Normalize values to 0-1 range for color mapping."""
    min_val = values.min()
    max_val = values.max()
    return (values - min_val) / (max_val - min_val)


def get_color(value, colormap='RdYlGn'):
    """
    Get color based on normalized value (0-1).
    
    Args:
        value: Normalized value between 0 and 1
        colormap: Color scheme ('RdYlGn', 'viridis', 'plasma', 'coolwarm')
    
    Returns:
        Hex color string
    """
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    
    if colormap == 'RdYlGn':
        # Red (slow) to Yellow to Green (fast)
        cmap = cm.RdYlGn
    elif colormap == 'viridis':
        cmap = cm.viridis
    elif colormap == 'plasma':
        cmap = cm.plasma
    elif colormap == 'coolwarm':
        cmap = cm.coolwarm
    else:
        cmap = cm.viridis
    
    rgba = cmap(value)
    return mcolors.rgb2hex(rgba)


# create map

def create_marathon_map(df, value_column='speed', colormap='RdYlGn_r'):
    """
    Create interactive map with color-coded route.
    
    Args:
        df: DataFrame with latitude, longitude, and value columns
        value_column: Column name to use for coloring (e.g., 'speed', 'heart_rate')
        colormap: Color scheme to use
    
    Returns:
        folium.Map object
    """
    
    # Calculate center of the route
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    # Create map centered on route
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='OpenStreetMap'
    )
    
    # Normalize values for color mapping
    normalized_values = normalize_values(df[value_column])
    
    # Add colored markers for each point
    for idx, row in df.iterrows():
        color = get_color(normalized_values.iloc[idx], colormap)
        
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=3,
            popup=f"{value_column.title()}: {row[value_column]:.2f}",
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=1
        ).add_to(m)
    
    # Add start marker
    folium.Marker(
        location=[df.iloc[0]['latitude'], df.iloc[0]['longitude']],
        popup='Start',
        icon=folium.Icon(color='green', icon='play')
    ).add_to(m)
    
    # Add finish marker
    folium.Marker(
        location=[df.iloc[-1]['latitude'], df.iloc[-1]['longitude']],
        popup='Finish',
        icon=folium.Icon(color='red', icon='stop')
    ).add_to(m)
    
    # Add color legend
    legend_html = f'''
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 150px; height: 90px; 
                background-color: white; z-index:9999; font-size:14px;
                border:2px solid grey; border-radius: 5px; padding: 10px">
    <p style="margin: 0;"><b>{value_column.title()} Scale</b></p>
    <p style="margin: 5px 0;">High: {df[value_column].max():.2f}</p>
    <p style="margin: 5px 0;">Low: {df[value_column].min():.2f}</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


# main
if __name__ == "__main__":
    
    # Option 1: Dots/circles map
    print("Creating color-coded dots map...")
    map_dots = create_marathon_map(
        marathon_stream,
        value_column='heartrate',  # Change to 'heart_rate', 'altitude', etc.
        colormap='RdYlGn_r'
    )
    map_dots.save('visualizations/marathon_map_HR.html')
    print("✅ Saved: marathon_map_HR.html")
    
    print("\nOpen the HTML files in your browser to view the interactive maps!")