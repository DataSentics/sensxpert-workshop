# Databricks notebook source
# MAGIC %md # GPU & Large models
# MAGIC Showcase of how you can use GPU in databricks
# MAGIC 1. Create a GPU cluster
# MAGIC 2. Be awesome!

# COMMAND ----------

import tensorflow as tf
import numpy as np

# COMMAND ----------

print("GPU Available:", tf.test.is_gpu_available())

# COMMAND ----------

# Generate random data for demonstration
np.random.seed(42) 
X_train = np.random.rand(100, 10) 
y_train = np.random.randint(0, 2, size=(100,)) 

# COMMAND ----------

# Define a simple neural network model
model = tf.keras.Sequential([tf.keras.layers.Dense(32, activation='relu', input_shape=(10,)),     tf.keras.layers.Dense(1, activation='sigmoid') ]) 
# Compile the model
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy']) # Train the model on GPU

# COMMAND ----------

with tf.device('/GPU:0'):     
    model.fit(X_train, y_train, epochs=10, batch_size=32) 

# COMMAND ----------

X_test = np.random.rand(20, 10) 
y_test = np.random.randint(0, 2, size=(20,))
with tf.device('/GPU:0'):
    loss, accuracy = model.evaluate(X_test, y_test)     
    print("Test loss:", loss)     
    print("Test accuracy:", accuracy)
