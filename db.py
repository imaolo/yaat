import subprocess
import pymongo

mongod_process = None

def start_up():
    global mongod_process
    try:
        # Start MongoDB daemon
        mongod_process = subprocess.Popen(['mongod', '--dbpath', 'data/db'])

        # Wait for the process to terminate (if needed)
        mongod_process.wait()


        client = pymongo.MongoClient()
        if 'tickers' in client.list_database_names():
            print("Database 'tickers' exists.")
        else:
            # Create the 'tickers' database
            client['tickers'].create_database('tickers')
            print("Database 'tickers' created.")
    except Exception as e:
        json_error={'Error':f'{e}'}
        print(json_error)
        return json_error


def stop():
    global mongod_process
    try:
        # Perform operations with MongoDB

        # Terminate MongoDB subprocess
        if mongod_process:
            print('terminating mongod instance')
            mongod_process.terminate()
    except Exception as e:
        json_error={'Error':f'{e}'}
        print(json_error)
        return json_error


# Example usage
start_up()


stop()


