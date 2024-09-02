from db.db_connections import create_connection
from db.create_tables import create_tables
from data.load_data import load_data
from db.insert_data import insert_covid_data, insert_vaccination_data
from db.queries import run_queries
# from visualizations.plot_data import plot_vaccination_rate

def main():
    conn = create_connection()
    
    # Create tables
    create_tables(conn)
    
    # Load and insert data
    covid_data = load_data('data/CovidDeaths.csv')
    vaccination_data = load_data('data/CovidVaccinations.csv')
    
    insert_covid_data(conn, covid_data)
    insert_vaccination_data(conn, vaccination_data)
    table_name = 'covid_data'
    
    # Run queries and visualize
    # # Define your SQL query
    query = f'''
        SELECT *
        FROM {table_name} order by 4, 5
    '''

    db_data = run_queries(query, conn)
    if not db_data.empty:
        # Proceed with data processing or visualization
        print(db_data.head())
    else:
        print("No data retrieved.")
    
    # plot_vaccination_rate(db_data)
    

if __name__ == "__main__":
    main()
