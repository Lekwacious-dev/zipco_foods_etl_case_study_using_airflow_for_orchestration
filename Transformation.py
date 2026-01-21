import pandas as pd

def run_transformation():
    data = pd.read_csv('zipco_transaction.csv')

    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handle missing values (filling missing numeric values with mean or medium)
    numeric_colums = data.select_dtypes(include=['Float64', 'int64']).columns
    for col in numeric_colums:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handle missing values  (fill missing string/object values with 'unknown')

    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    # cleaning Date column/change date to the right dataTypt "datetime64"
    data['Date'] = pd.to_datetime(data['Date'])

    # Create Product table
    products = data[['ProductName', 'PromotionApplied']].drop_duplicates().reset_index(drop=True)
    # start index from 1
    products.index = products.index + 1
    products.index.name = 'ProductID'
    # convert index to column
    products = products.reset_index()

    # Customer Table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber',
        'CustomerEmail','CustomerFeedback']].drop_duplicates().reset_index(drop=True)

    # start index from 1
    customers.index = customers.index + 1
    customers.index.name = 'CustomerID'
    # convert index to column
    customers = customers.reset_index()


    # staff Table
    staffs = data[['Staff_Name', 'Staff_Email', 'StaffPerformanceRating']].drop_duplicates().reset_index(drop=True)

    # start index from 1
    staffs.index = staffs.index + 1
    staffs.index.name = 'StaffID'

    # convert index to column
    staffs = staffs.reset_index() 


    # Weather Table
    weather = data[['Weather', 'Temperature']].drop_duplicates().reset_index(drop=True)

    # start index from 1
    weather.index = weather.index + 1
    weather.index.name = 'WeatherID'

    weather = weather.reset_index()


    # transaction table
    transactions = data.merge(products, on=['ProductName', 'PromotionApplied'], how='left') \
                        .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail','CustomerFeedback'], how='left') \
                        .merge(staffs, on=['Staff_Name', 'Staff_Email', 'StaffPerformanceRating'], how='left') \
                        .merge(weather, on=['Weather', 'Temperature'], how='left') 
    transactions.index = transactions.index + 1
    transactions.index.name = 'TransactionID'

    transactions = transactions.reset_index() \
                                [['Date', 'TransactionID', 'Quantity', 'UnitPrice', 'StoreLocation', \
                                    'PaymentType', 'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales', \
                                    'CustomerID', 'StaffID', 'WeatherID', 'ProductID']]
    

    # Save data as csv files
    data.to_csv('dataset/cleandata/clean_data.csv', index=False)
    customers.to_csv('dataset/cleandata/customers.csv', index=False)
    products.to_csv('dataset/cleandata/products.csv', index=False)
    staffs.to_csv('dataset/cleandata/staffs.csv', index=False)
    weather.to_csv('dataset/cleandata/weather.csv', index=False)
    transactions.to_csv('dataset/cleandata/transactions.csv', index=False)


    print('Data Cleaning and Transformation completed successfully!')