# Databricks notebook source
# MAGIC %md
# MAGIC # Decorators
# MAGIC * [Documentation](https://python101.pythonlibrary.org/chapter25_decorators.html)
# MAGIC * Decorators are a very powerful and useful tool in Python since it allows programmers to modify the behaviour of a function or class. 
# MAGIC * Decorators allow us to wrap another function in order to extend the behaviour of the wrapped function, without permanently modifying it.
# MAGIC * In Decorators, functions are taken as the argument into another function and then called inside the wrapper function.

# COMMAND ----------

# DBTITLE 1,Example of decorator - calculating function execution time
# importing libraries
import time
import math
 
# decorator to calculate duration
# taken by any function.
def calculate_time(func):
     
    # added arguments inside the inner1,
    # if function takes any arguments,
    # can be added like this.
    def inner1(*args, **kwargs):
 
        # storing time before function execution
        begin = time.time()
         
        func(*args, **kwargs)
 
        # storing time after function execution
        end = time.time()
        print("Total time taken in : ", func.__name__, end - begin)
 
    return inner1
 
 
 
# this can be added to any function present,
# in this case to calculate a factorial
@calculate_time
def factorial(num):
    print(math.factorial(num))
 
# calling the function.
factorial(10)

# COMMAND ----------

# DBTITLE 1,Example of decorator returning value

def hello_decorator(func):
    def inner1(*args, **kwargs):
         
        print("before Execution")
         
        # getting the returned value
        returned_value = func(*args, **kwargs)
        print("after Execution")
         
        # returning the value to the original frame
        return returned_value
         
    return inner1
 
 
# adding decorator to the function
@hello_decorator
def sum_two_numbers(a, b):
    print("Inside the function")
    return a + b
 
a, b = 1, 2
 
# getting the value through return of the function
print("Sum =", sum_two_numbers(a, b))

# COMMAND ----------

# DBTITLE 1,Example of multiple decorators
def decor1(func):
    def inner():
        x = func()
        return x * x
    return inner
 
def decor(func):
    def inner():
        x = func()
        return 2 * x
    return inner
 
@decor1
@decor
def num():
    return 10

# decor1(decor(num()))
# first is executed function decor -> 2 * 10 = 20
# then is executed function decor1 -> 20 * 20 = 400
print(num())

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC * [Source of examples](https://www.geeksforgeeks.org/decorators-in-python/)
