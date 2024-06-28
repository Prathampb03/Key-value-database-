import etcd3
import grpc

# Function to establish connection to etcd
def connect_to_etcd():
    try:
        etcd_client = etcd3.client(host='localhost', port=2379)
        return etcd_client
    except grpc.RpcError as e:
        print("Connection Error: Unable to connect to the etcd cluster.")
        print("Error:", e.details())
        return None

# Function to list all keys in etcd
def list_all_keys(etcd_client):
    try:
        keys_metadata = etcd_client.get_all()
        return [m.key.decode() for (_, m) in keys_metadata]
    except etcd3.exceptions.ConnectionFailedError:
        print("Connection Error: Unable to retrieve keys. Please check your connection to the etcd cluster.")
        return None

# Function to get the value for a specific key provided by the user
def get_value_for_key(etcd_client, key):
    try:
        value, _ = etcd_client.get(key)
        return value.decode() if value else None
    except etcd3.exceptions.KeyNotFoundError:
        print("Key Error: Key '{}' not found in etcd.".format(key))
        return None
    except etcd3.exceptions.ConnectionFailedError:
        print("Connection Error: Unable to retrieve value for key. Please check your connection to the etcd cluster.")
        return None

# Function to put a key-value pair into etcd
def put_key_value(etcd_client, key, value):
    try:
        etcd_client.put(key, value)
        print("\nKey-Value pair successfully added to etcd.")
    except etcd3.exceptions.ConnectionFailedError:
        print("Connection Error: Unable to put key-value pair. Please check your connection to the etcd cluster.")

# Function to delete a key-value pair from etcd based on the given key
def delete_key(etcd_client, key):
    try:
        # Check if the key exists before attempting to delete it
        value, _ = etcd_client.get(key)
        if value is None:
            print("Key Error: Key '{}' not found in etcd.".format(key))
        else:
            etcd_client.delete(key)
            print("\nKey-Value pair successfully deleted from etcd.")
    except etcd3.exceptions.ConnectionFailedError:
        print("Connection Error: Unable to delete key-value pair. Please check your connection to the etcd cluster.")

# Main function to handle user options
def main():
    etcd_client = connect_to_etcd()
    if etcd_client is None:
        return

    while True:
        print("\nChoose an option:")
        print("1. List all keys")
        print("2. Get value for a specific key")
        print("3. Put a key-value pair")
        print("4. Delete a key-value pair")
        print("Press Ctrl+C to exit")
        print("-" * 20)  # Dashed line

        option = input("Enter your choice (1/2/3/4): ")
        
        try:
            if option == '1':
                keys = list_all_keys(etcd_client)
                if keys is not None:
                    print("\nAll Keys in etcd:")
                    print("-" * 20)  # Dashed line
                    for key in keys:
                        print(key)
            elif option == '2':
                user_key = input("Enter the key to get its value: ")
                value = get_value_for_key(etcd_client, user_key)
                if value is not None:
                    print("\nValue for key '{}':".format(user_key))
                    print("-" * 20)  # Dashed line
                    print(value)
            elif option == '3':
                new_key = input("Enter the key to put into etcd: ")
                new_value = input("Enter the value for the key: ")
                put_key_value(etcd_client, new_key, new_value)
            elif option == '4':
                delete_key_input = input("Enter the key to delete from etcd: ")
                delete_key(etcd_client, delete_key_input)
            else:
                print("\nInvalid option. Please choose a valid option.")
        except KeyboardInterrupt:
            print("\nExiting program...")
            break

# Entry point of the script
if __name__ == "__main__":
    main()