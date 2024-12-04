import requests
import pandas as pd
import psycopg2


def dms(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        extracted_data = [
            {
                "FinancialyearID":item["FinancialyearID"],
                "GroupingName":item["GroupingName"],
                "RegionName":item["RegionName"],
                "DistrictName":item["DistrictName"],
                "MuncipalityName":item["MuncipalityName"],
                "GMName":item["GMName"],
                "Date":item["Date"],
                "TotalAssesments":item["TotalAssesments"],
                "TotalDemand":item["TotalDemand"],
                "TotalCollection":item["TotalCollection"],
                "TotalBal":item["TotalBal"],
                "%Collection":item["%Collection"]
            }
            for item in data
        ]
        return pd.DataFrame(extracted_data)
    else:
        raise Exception(f"Failed to fetch data. Status Code: {response.status_code}")


def csv(df, file_name):
    df.to_csv(file_name, index=False)
    print(f"Data successfully saved to {file_name}")

def normalize_name(name):
    """
    Normalize the district name for consistent comparison.
    """
    if name is None:
        return None
    name = name.strip().lower()  # Case insensitive and remove leading/trailing spaces
    
    # Replace specific known mismatches
    corrections = {
        "tiruchirappalli": "thiruchirappalli",
        "kanyakumari": "kanniyakumari",
        # Add other known corrections as needed
    }
    
    return corrections.get(name, name)  # Default to the name if no correction is found


def main():
    url = "https://tnurbanepay.tn.gov.in/api/WW_Dashboard/CM_dashboard_DCB?orgtype=6"
    db_config = {
        "dbname": "test",
        "user": "admin",
        "password": "godspeed123",
        "host": "192.168.50.26",
        "port": "5431"
    }

    try:
        df = dms(url)
        csv(df, 'dma_tax.csv')

        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

       
    except Exception as e:
        print("An error occurred:", e)

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
