import mysql.connector
from pybeepop.to_ddl import * 


def test_init_tables():
    """
    test the initialization fo the tables
    """
    assert(init_tables("tests/scripts/output.sql"), True)


def test_MonitorStation():
    """
    test the create MonitorStation table function
    """
    assert(create_sql_MonitorStation("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv", "scripts/output.sql"), True)

    #input path that do not exist
    assert (create_sql_MonitorStation("tests/data/processed/average_monthly_temperature_by_state_1950-2021.csv", "scripts/output.sql"), False)


def test_Bee():
    """
    test the create bee table function
    """
    assert(create_sql_Bee("tests/data/processed/save_the_bees.csv", "scripts/output.sql"), True)

    #input path that do not exist
    assert(create_sql_Bee("tests/data/processed/save_the_bee.csv", "scripts/output.sql"), False)


def test_detect():
    """
    test the create detect table function
    """
    assert(create_sql_detect("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv", "data/processed/save_the_bees.csv", "scripts/output.sql"), True)

    #first input path do not exist
    assert(create_sql_detect("tests/data/processed/average_monthly_temperature_by_state_1950-2021.csv", "data/processed/save_the_bees.csv", "scripts/output.sql"), False)

    #second input path do not exist
    assert(create_sql_detect("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv", "data/processed/save_the_bee.csv", "scripts/output.sql"), False)

    #both input pathes do not exist 
    assert(create_sql_detect("tests/data/processed/average_monthly_temperature_by_state_1950-2021.csv", "data/processed/save_the_bee.csv", "scripts/output.sql"), False)


def test_GasConditions():
    """
    test the create GasConditions table function
    """
    assert(create_sql_GasConditions("tests/data/processed/pollution_2000_2021.csv", "scripts/output.sql"), True)

    #input path that do not exist
    assert(create_sql_GasConditions("tests/data/processed/pollution_2000_2022.csv", "scripts/output.sql"), False)


def test_Influence():
    """
    test the create Influence table function
    """
    assert(create_sql_Influence("tests/data/processed/pollution_2000_2021.csv", "tests/data/processed/save_the_bees.csv", "tests/scripts/output.sql"), True)

    #first input path do not exist
    assert(create_sql_Influence("tests/data/processed/pollution_2000_2022.csv", "tests/data/processed/save_the_bees.csv", "tests/scripts/output.sql"), False)

    #second input path do not exist
    assert(create_sql_Influence("tests/data/processed/pollution_2000_2021.csv", "tests/data/processed/save_the_bee.csv", "tests/scripts/output.sql"), False)

    #both input pathes do not exist 
    assert(create_sql_Influence("tests/data/processed/pollution_2000_2022.csv", "tests/data/processed/save_the_bee.csv", "tests/scripts/output.sql"), False)


def test_RiskFactors():
    """
    test the create RiskFactors table function
    """
    assert(create_sql_RiskFactors("tests/data/processed/helper.csv", "tests/scripts/output.sql"), True)

    #input path that do not exist
    assert(create_sql_RiskFactors("tests/data/processed/helper1.csv", "tests/scripts/output.sql"), False)


def test_Monitor():
    """
    test the create Monitor table function
    """
    assert(create_sql_Monitor("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv","tests/data/processed/helper.csv", "tests/scripts/output.sql"), True)

    #first input path do not exist
    assert(create_sql_Monitor("tests/data/processed/average_monthly_temperature_by_state_1950-2021.csv","tests/data/processed/helper.csv", "tests/scripts/output.sql"), False)

    #second input path do not exist
    assert(create_sql_Monitor("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv","tests/data/processed/helper1.csv", "tests/scripts/output.sql"), False)

    #both input pathes do not exist 
    assert(create_sql_Monitor("tests/data/processed/average_monthly_temperature_by_state_1950-2021.csv","tests/data/processed/helper1.csv", "tests/scripts/output.sql"), False)


def test_Kill():
    """
    test the create Kill table function
    """
    assert(create_sql_Kill("tests/data/processed/save_the_bees.csv","tests/data/processed/pollution_2000_2021.csv", "tests/scripts/output.sql"), True)

    #first input path do not exist
    assert(create_sql_Kill("tests/data/processed/save_the_bee.csv","tests/data/processed/pollution_2000_2021.csv", "tests/scripts/output.sql"), False)

    #second input path do not exist
    assert(create_sql_Kill("tests/data/processed/save_the_bees.csv","tests/data/processed/pollution_2000_2020.csv", "tests/scripts/output.sql"), False)

    #both input pathes do not exist 
    assert(create_sql_Kill("tests/data/processed/save_the_bee.csv","tests/data/processed/pollution_2000_2020.csv", "tests/scripts/output.sql"), False)


def test_Parasite():
    """
    test the create Parasite table function
    """
    assert(create_sql_Parasite("tests/data/processed/save_the_bees.csv", "tests/scripts/output.sql"), True)

    #input path that do not exist
    assert(create_sql_Parasite("tests/data/processed/save_the_bee.csv", "tests/scripts/output.sql"), False)


def test_Pesticide():
    """
    test the create Pesticide table function
    """
    assert(create_sql_Pesticide("tests/data/processed/epest_county_estimates.csv", "tests/scripts/output.sql"), True)

    #input path that do not exist
    assert(create_sql_Pesticide("tests/data/processed/epest_county_estimate.csv", "tests/scripts/output.sql"), False)


def test_connect_db():
    """
    test connect_to_db()
    """
    connection_output = connect_to_db(3307, 'localhost')
    connection_test = mysql.connector.connect(
        host = 'localhost',
        user = 'system',
        password = '123456',
        database = 'bee_population_analysis_db',
        port = 3307
    )

    assert(connection_output, connection_test)


def test_load_sql_db():
    """
    test load load_sql_to_db()
    """
    init_tables("tests/scripts/output.sql")
    create_sql_MonitorStation("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv", "scripts/output.sql")
    create_sql_Bee("tests/data/processed/save_the_bees.csv", "scripts/output.sql")
    create_sql_detect("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv", "data/processed/save_the_bees.csv", "scripts/output.sql")
    create_sql_GasConditions("tests/data/processed/pollution_2000_2021.csv", "scripts/output.sql")
    create_sql_Influence("tests/data/processed/pollution_2000_2021.csv", "tests/data/processed/save_the_bees.csv", "tests/scripts/output.sql")
    create_sql_RiskFactors("tests/data/processed/helper.csv", "tests/scripts/output.sql")
    create_sql_Monitor("tests/data/processed/average_monthly_temperature_by_state_1950-2022.csv","tests/data/processed/helper.csv", "tests/scripts/output.sql")
    create_sql_Kill("tests/data/processed/save_the_bees.csv","tests/data/processed/pollution_2000_2021.csv", "tests/scripts/output.sql")
    create_sql_Parasite("tests/data/processed/save_the_bees.csv", "tests/scripts/output.sql")
    create_sql_Pesticide("tests/data/processed/epest_county_estimates.csv", "tests/scripts/output.sql")
    connection = connect_to_db(3307, 'localhost')
    assert(load_sql_to_db(connection, "tests/scripts/output.sql"), True)
    assert(load_sql_to_db(connection, "tests/scripts/output1.sql"), False)
