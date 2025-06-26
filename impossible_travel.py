import pandas as pd
import numpy as np

pd.set_option('display.float_format', '{:.2f}'.format)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi       = np.radians(lat2 - lat1)
    dlambda    = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))

def main():
    # Load the last-1-year NDJSON
    df = pd.read_json('bulk_synthetic_entra_signin_last_1_year copy.ndjson', lines=True)

    # Flatten JSON
    df = pd.json_normalize(df.to_dict(orient='records'), sep='.')

    # Parse timestamps & drop bad rows
    df['@timestamp'] = pd.to_datetime(df['@timestamp'], utc=True, errors='coerce')
    df = df.dropna(subset=['@timestamp','geo.location.lat','geo.location.lon'])

    # **New**: Filter to last 30 days
    now = pd.Timestamp.now(tz='UTC')
    df = df[df['@timestamp'] > now - pd.Timedelta(days=30)]

    df['login_day'] = df['@timestamp'].dt.normalize()
    df = df.sort_values(['user.name','@timestamp'])

    shifted = df.groupby(['user.name','login_day'])[
        ['geo.location.lat','geo.location.lon','@timestamp']
    ].shift()
    df['lat_prev'], df['lon_prev'], df['ts_prev'] = (
        shifted['geo.location.lat'],
        shifted['geo.location.lon'],
        shifted['@timestamp']
    )

    df['dist_km'] = haversine(
        df['lat_prev'], df['lon_prev'],
        df['geo.location.lat'], df['geo.location.lon']
    )
    df['hours'] = (df['@timestamp'] - df['ts_prev']).dt.total_seconds()/3600.0

    df['speed_kmh'] = df['dist_km']/df['hours']
    df.loc[df['hours']<=0,'speed_kmh'] = 0.0
    df['speed_kmh'] = df['speed_kmh'].fillna(0.0)

    result = df.groupby(['user.name','login_day']).agg(
        max_speed_kmh=('speed_kmh','max'),
        last_ts       =('@timestamp','max'),
        last_ip       =('client.ip','last'),
        last_lat      =('geo.location.lat','last'),
        last_lon      =('geo.location.lon','last'),
    ).reset_index()

    impossible_travel = (
        result[result['max_speed_kmh']>0]
        .sort_values('max_speed_kmh',ascending=False)
    )
    print(impossible_travel.head().to_string(index=False))

if __name__=='__main__':
    main()
