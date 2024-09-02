from psycopg2.extras import execute_values
import pandas as pd
from psycopg2 import sql


def insert_covid_data(conn, data):
    cursor = conn.cursor()
    insert_query = sql.SQL('''
        INSERT INTO covid_data (
            iso_code,
            continent,
            location,
            date,
            total_cases,
            new_cases,
            new_cases_smoothed,
            total_deaths,
            new_deaths,
            new_deaths_smoothed,
            total_cases_per_million,
            new_cases_per_million,
            new_cases_smoothed_per_million,
            total_deaths_per_million,
            new_deaths_per_million,
            new_deaths_smoothed_per_million,
            reproduction_rate,
            icu_patients,
            icu_patients_per_million,
            hosp_patients,
            hosp_patients_per_million,
            weekly_icu_admissions,
            weekly_icu_admissions_per_million,
            weekly_hosp_admissions,
            weekly_hosp_admissions_per_million,
            new_tests,
            total_tests,
            total_tests_per_thousand,
            new_tests_per_thousand,
            new_tests_smoothed,
            new_tests_smoothed_per_thousand,
            positive_rate,
            tests_per_case,
            tests_units,
            total_vaccinations,
            people_vaccinated,
            people_fully_vaccinated,
            new_vaccinations,
            new_vaccinations_smoothed,
            total_vaccinations_per_hundred,
            people_vaccinated_per_hundred,
            people_fully_vaccinated_per_hundred,
            new_vaccinations_smoothed_per_million,
            stringency_index,
            population,
            population_density,
            median_age,
            aged_65_older,
            aged_70_older,
            gdp_per_capita,
            extreme_poverty,
            cardiovasc_death_rate,
            diabetes_prevalence,
            female_smokers,
            male_smokers,
            handwashing_facilities,
            hospital_beds_per_thousand,
            life_expectancy,
            human_development_index
        ) VALUES %s
    ''')
    
    data_to_insert = []
    for index, row in data.iterrows():
        try:
            # Replace NaN with None for insertion
            row_data = (
                row['iso_code'],
                row['continent'],
                row['location'],
                pd.to_datetime(row['date']).strftime('%Y-%m-%d'),  # Format date
                row['total_cases'] if pd.notna(row['total_cases']) else None,
                row['new_cases'] if pd.notna(row['new_cases']) else None,
                row['new_cases_smoothed'] if pd.notna(row['new_cases_smoothed']) else None,
                row['total_deaths'] if pd.notna(row['total_deaths']) else None,
                row['new_deaths'] if pd.notna(row['new_deaths']) else None,
                row['new_deaths_smoothed'] if pd.notna(row['new_deaths_smoothed']) else None,
                row['total_cases_per_million'] if pd.notna(row['total_cases_per_million']) else None,
                row['new_cases_per_million'] if pd.notna(row['new_cases_per_million']) else None,
                row['new_cases_smoothed_per_million'] if pd.notna(row['new_cases_smoothed_per_million']) else None,
                row['total_deaths_per_million'] if pd.notna(row['total_deaths_per_million']) else None,
                row['new_deaths_per_million'] if pd.notna(row['new_deaths_per_million']) else None,
                row['new_deaths_smoothed_per_million'] if pd.notna(row['new_deaths_smoothed_per_million']) else None,
                row['reproduction_rate'] if pd.notna(row['reproduction_rate']) else None,
                row['icu_patients'] if pd.notna(row['icu_patients']) else None,
                row['icu_patients_per_million'] if pd.notna(row['icu_patients_per_million']) else None,
                row['hosp_patients'] if pd.notna(row['hosp_patients']) else None,
                row['hosp_patients_per_million'] if pd.notna(row['hosp_patients_per_million']) else None,
                row['weekly_icu_admissions'] if pd.notna(row['weekly_icu_admissions']) else None,
                row['weekly_icu_admissions_per_million'] if pd.notna(row['weekly_icu_admissions_per_million']) else None,
                row['weekly_hosp_admissions'] if pd.notna(row['weekly_hosp_admissions']) else None,
                row['weekly_hosp_admissions_per_million'] if pd.notna(row['weekly_hosp_admissions_per_million']) else None,
                row['new_tests'] if pd.notna(row['new_tests']) else None,
                row['total_tests'] if pd.notna(row['total_tests']) else None,
                row['total_tests_per_thousand'] if pd.notna(row['total_tests_per_thousand']) else None,
                row['new_tests_per_thousand'] if pd.notna(row['new_tests_per_thousand']) else None,
                row['new_tests_smoothed'] if pd.notna(row['new_tests_smoothed']) else None,
                row['new_tests_smoothed_per_thousand'] if pd.notna(row['new_tests_smoothed_per_thousand']) else None,
                row['positive_rate'] if pd.notna(row['positive_rate']) else None,
                row['tests_per_case'] if pd.notna(row['tests_per_case']) else None,
                row['tests_units'] if pd.notna(row['tests_units']) else None,
                row['total_vaccinations'] if pd.notna(row['total_vaccinations']) else None,
                row['people_vaccinated'] if pd.notna(row['people_vaccinated']) else None,
                row['people_fully_vaccinated'] if pd.notna(row['people_fully_vaccinated']) else None,
                row['new_vaccinations'] if pd.notna(row['new_vaccinations']) else None,
                row['new_vaccinations_smoothed'] if pd.notna(row['new_vaccinations_smoothed']) else None,
                row['total_vaccinations_per_hundred'] if pd.notna(row['total_vaccinations_per_hundred']) else None,
                row['people_vaccinated_per_hundred'] if pd.notna(row['people_vaccinated_per_hundred']) else None,
                row['people_fully_vaccinated_per_hundred'] if pd.notna(row['people_fully_vaccinated_per_hundred']) else None,
                row['new_vaccinations_smoothed_per_million'] if pd.notna(row['new_vaccinations_smoothed_per_million']) else None,
                row['stringency_index'] if pd.notna(row['stringency_index']) else None,
                row['population'] if pd.notna(row['population']) else None,
                row['population_density'] if pd.notna(row['population_density']) else None,
                row['median_age'] if pd.notna(row['median_age']) else None,
                row['aged_65_older'] if pd.notna(row['aged_65_older']) else None,
                row['aged_70_older'] if pd.notna(row['aged_70_older']) else None,
                row['gdp_per_capita'] if pd.notna(row['gdp_per_capita']) else None,
                row['extreme_poverty'] if pd.notna(row['extreme_poverty']) else None,
                row['cardiovasc_death_rate'] if pd.notna(row['cardiovasc_death_rate']) else None,
                row['diabetes_prevalence'] if pd.notna(row['diabetes_prevalence']) else None,
                row['female_smokers'] if pd.notna(row['female_smokers']) else None,
                row['male_smokers'] if pd.notna(row['male_smokers']) else None,
                row['handwashing_facilities'] if pd.notna(row['handwashing_facilities']) else None,
                row['hospital_beds_per_thousand'] if pd.notna(row['hospital_beds_per_thousand']) else None,
                row['life_expectancy'] if pd.notna(row['life_expectancy']) else None,
                row['human_development_index'] if pd.notna(row['human_development_index']) else None
            )

            # Append to data_to_insert
            data_to_insert.append(row_data)
            
        except Exception as e:
            print(f'Error processing row {index}: {e}')

    # Insert the data into the database
    execute_values(cursor, insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insert_vaccination_data(conn, data):
    cursor = conn.cursor()
    insert_new_query = sql.SQL('''
        INSERT INTO vaccin_data (
            iso_code,
            continent,
            location,
            date,
            new_tests,
            total_tests,
            total_tests_per_thousand,
            new_tests_per_thousand,
            new_tests_smoothed,
            new_tests_smoothed_per_thousand,
            positive_rate,
            tests_per_case,
            tests_units,
            total_vaccinations,
            people_vaccinated,
            people_fully_vaccinated,
            new_vaccinations,
            new_vaccinations_smoothed,
            total_vaccinations_per_hundred,
            people_vaccinated_per_hundred,
            people_fully_vaccinated_per_hundred,
            new_vaccinations_smoothed_per_million,
            stringency_index,
            population_density,
            median_age,
            aged_65_older,
            aged_70_older,
            gdp_per_capita,
            extreme_poverty,
            cardiovasc_death_rate,
            diabetes_prevalence,
            female_smokers,
            male_smokers,
            handwashing_facilities,
            hospital_beds_per_thousand,
            life_expectancy,
            human_development_index
        ) VALUES %s
    ''')

    
    data_to_insert = []
    for index, row in data.iterrows():
        try:
            row_data = (
                row['iso_code'],
                row['continent'],
                row['location'],
                pd.to_datetime(row['date']).strftime('%Y-%m-%d'),  # Format date
                row['new_tests'] if pd.notna(row['new_tests']) else None,
                row['total_tests'] if pd.notna(row['total_tests']) else None,
                row['total_tests_per_thousand'] if pd.notna(row['total_tests_per_thousand']) else None,
                row['new_tests_per_thousand'] if pd.notna(row['new_tests_per_thousand']) else None,
                row['new_tests_smoothed'] if pd.notna(row['new_tests_smoothed']) else None,
                row['new_tests_smoothed_per_thousand'] if pd.notna(row['new_tests_smoothed_per_thousand']) else None,
                row['positive_rate'] if pd.notna(row['positive_rate']) else None,
                row['tests_per_case'] if pd.notna(row['tests_per_case']) else None,
                row['tests_units'] if pd.notna(row['tests_units']) else None,
                row['total_vaccinations'] if pd.notna(row['total_vaccinations']) else None,
                row['people_vaccinated'] if pd.notna(row['people_vaccinated']) else None,
                row['people_fully_vaccinated'] if pd.notna(row['people_fully_vaccinated']) else None,
                row['new_vaccinations'] if pd.notna(row['new_vaccinations']) else None,
                row['new_vaccinations_smoothed'] if pd.notna(row['new_vaccinations_smoothed']) else None,
                row['total_vaccinations_per_hundred'] if pd.notna(row['total_vaccinations_per_hundred']) else None,
                row['people_vaccinated_per_hundred'] if pd.notna(row['people_vaccinated_per_hundred']) else None,
                row['people_fully_vaccinated_per_hundred'] if pd.notna(row['people_fully_vaccinated_per_hundred']) else None,
                row['new_vaccinations_smoothed_per_million'] if pd.notna(row['new_vaccinations_smoothed_per_million']) else None,
                row['stringency_index'] if pd.notna(row['stringency_index']) else None,
                row['population_density'] if pd.notna(row['population_density']) else None,
                row['median_age'] if pd.notna(row['median_age']) else None,
                row['aged_65_older'] if pd.notna(row['aged_65_older']) else None,
                row['aged_70_older'] if pd.notna(row['aged_70_older']) else None,
                row['gdp_per_capita'] if pd.notna(row['gdp_per_capita']) else None,
                row['extreme_poverty'] if pd.notna(row['extreme_poverty']) else None,
                row['cardiovasc_death_rate'] if pd.notna(row['cardiovasc_death_rate']) else None,
                row['diabetes_prevalence'] if pd.notna(row['diabetes_prevalence']) else None,
                row['female_smokers'] if pd.notna(row['female_smokers']) else None,
                row['male_smokers'] if pd.notna(row['male_smokers']) else None,
                row['handwashing_facilities'] if pd.notna(row['handwashing_facilities']) else None,
                row['hospital_beds_per_thousand'] if pd.notna(row['hospital_beds_per_thousand']) else None,
                row['life_expectancy'] if pd.notna(row['life_expectancy']) else None,
                row['human_development_index'] if pd.notna(row['human_development_index']) else None
            )
            data_to_insert.append(row_data)
        except Exception as e:
            print(f"Error processing row {index}: {e}")


    # Insert the data into the database
    execute_values(cursor, insert_new_query, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()
