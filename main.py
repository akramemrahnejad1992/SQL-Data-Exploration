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
    
   # # Define your SQL query
    # query = '''
    # SELECT *
    # FROM covid_data order by 4, 5
    # '''
    
    # # select data that we are going to be using
    # query = '''
    #     SELECT location, date, total_cases, new_cases, total_deaths, population
    #     FROM covid_data order by 1, 2
    # '''
    
    # # looking at total_cases vs total_deaths
    # query = """
    #     SELECT location, date, total_deaths, (NULLIF(total_deaths, 0) / CAST(total_cases AS FLOAT)) * 100
    #     FROM covid_data 
    #     order by 1, 2 
    # """
    
    
    # # show likelihood of dying in your country because of covid
    # query = """
    #     SELECT location, date, total_cases, total_deaths, (NULLIF(total_deaths, 0) / CAST(total_cases AS FLOAT)) * 100 AS death_rate
    #     FROM covid_data 
    #     WHERE LOWER(location) LIKE '%states%'
    #     order by 1, 2 
    # """
    
    
    
    # # shows what percentage of population got covid
    # query = """
    #     SELECT location, date, total_cases, population, (NULLIF(total_cases, 0) / CAST(population AS FLOAT)) * 100 AS infection_rate
    #     FROM covid_data 
    #     --WHERE LOWER(location) LIKE '%states%'
    #     order by 1, 2 
    # """
    
    
    # # looking at countries with highest infection rate compared to population
    # query = """
    #     SELECT location, population, MAX(total_cases) as highest_infection_count, MAX(NULLIF(total_cases, 0) / CAST(population AS FLOAT)) * 100 AS infection_rate
    #     FROM covid_data 
    #     -- WHERE LOWER(location) LIKE '%states%'
    #     Group by(location, population)
    #     order by infection_rate desc
    # """
    
    
    # # looking at countries with highest death rate compared to population
    # query = """
    #     SELECT location, MAX(CAST(total_deaths AS INT)) as highest_death_count
    #     FROM covid_data 
    #     WHERE continent <> 'NaN'
    #     -- WHERE continent = 'NaN'
    #     -- WHERE continent is not null
    #     Group by location
    #     order by highest_death_count desc  
    # """
    
    # # break things down by continent 
    # query = """
    #     SELECT continent, MAX(CAST(total_deaths AS INT)) as highest_death_count
    #     FROM covid_data 
    #     WHERE continent = 'NaN'
    #     -- WHERE continent is not null
    #     Group by continent
    #     order by highest_death_count desc 
    # """
    
    
    # # showing coninent with highest death count compared to population
    # query = """
    #     SELECT continent, MAX(CAST(total_deaths AS INT)) as highest_death_count
    #     FROM covid_data 
    #     WHERE continent <> 'NaN'
    #     -- WHERE continent = 'NaN'
    #     -- WHERE continent is not null
    #     Group by continent
    #     order by highest_death_count desc  
    # """
    
    # # GLOBAL numbers
    # query = """
    #     -- SELECT date, total_cases
    #     SELECT SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths, (SUM(new_deaths)/SUM(new_cases)) * 100 as global_death_rate
    #     FROM covid_data 
    #     WHERE continent <> 'NaN'
    #     -- WHERE continent = 'NaN'
    #     -- WHERE continent is not null
    #     -- GROUP BY date
    #     -- order by date
    # """

    db_data = run_queries(query, conn)
    if not db_data.empty:
        # Proceed with data processing or visualization
        print(db_data.head())
    else:
        print("No data retrieved.")
    
    # plot_vaccination_rate(db_data)
    

if __name__ == "__main__":
    main()
