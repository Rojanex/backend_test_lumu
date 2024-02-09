# backend_test_lumu

# How to run the code

1. Install necessary libraries from requirements.txt
2. Create an environment file (.env) in the root where you declare LUMU_CLIENT_KEY, COLLECTOR_ID, BASE_URL (lumu api)
3. Run python file "run.py" and as a second arg, add filepath of logs:
    Example:  python3 run.py /Users/rojanex/Downloads/queries 


Result example:

    ![My Capture](/capture.jpg)

# Computational complexity of ranking algorithm

    - Creating a list only with the client_ip value of the record list has a complexity of O(n)

    - Creating a Counter object, to count how many times each ip appears in the list has a complexity of O(n)

    -  Using most_common() method of counter object to obtain only top 5 ranks has a complexity of O(n log n)

Therefore, the complexity is O(n log n)

