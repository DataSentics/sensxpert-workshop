# Databricks notebook source
# MAGIC %md ## Question answering example

# COMMAND ----------

# MAGIC %pip install tensorflow

# COMMAND ----------

!pip install transformers # == huggingface

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from transformers import AutoTokenizer, AutoModelForQuestionAnswering, pipeline
import torch
import tensorflow as tf
model_name = 'deepset/roberta-base-squad2'
context = 'The best data platform is Databricks.'

# COMMAND ----------

model = AutoModelForQuestionAnswering.from_pretrained(model_name, proxies = { 'https' : 'http://ngproxy-ecm.csin.cz:8080' })
tokenizer = AutoTokenizer.from_pretrained(model_name, proxies = { 'https' : 'http://ngproxy-ecm.csin.cz:8080' }, cache='dbfs:/huggingface_transformers_cache/')

# COMMAND ----------

def answer_question(model, tokenizer, question, context):
    inputs = tokenizer.encode_plus(question, context, add_special_tokens=True, truncation=True, padding='max_length', max_length=512, return_tensors='pt')
    with torch.no_grad():
        outputs = model(**inputs)
    start = int(tf.math.argmax(outputs.start_logits, axis=-1)[0])
    end = int(tf.math.argmax(outputs.end_logits, axis=-1)[0])
    answer_tokens = inputs.input_ids[0, start:end+1]
    print(f'Q: {question}')
    print(f'A: {tokenizer.decode(answer_tokens)}')


# COMMAND ----------

context = 'Machine learning (ML) is a field of study in artificial intelligence concerned with the development and study of statistical algorithms that can learn from data and generalize to unseen data, and thus perform tasks without explicit instructions. Recently, generative artificial neural networks have been able to surpass many previous approaches in performance.Machine learning approaches have been applied to many fields including large language models, computer vision, speech recognition, email filtering, agriculture, and medicine, where it is too costly to develop algorithms to perform the needed tasks. ML is known in its application across business problems under the name predictive analytics. Although not all machine learning is statistically based, computational statistics is an important source of the field\'s methods.\nThe mathematical foundations of ML are provided by mathematical optimization (mathematical programming) methods. Data mining is a related (parallel) field of study, focusing on exploratory data analysis through unsupervised learning. From a theoretical point of view Probably approximately correct learning provides a framework for describing machine learning.\n\n\n== History and relationships to other fields ==\n\nThe term machine learning was coined in 1959 by Arthur Samuel, an IBM employee and pioneer in the field of computer gaming and artificial intelligence."

# COMMAND ----------

question = "What is machine learning?"
answer_question(model, tokenizer, question, context)

# COMMAND ----------

context = "Beyoncé Giselle Knowles-Carter (/biːˈjɒnseɪ/ bee-YON-say) (born September 4, 1981) is an American singer, songwriter, record producer and actress. Born and raised in Houston, Texas, she performed in various singing and dancing competitions as a child, and rose to fame in the late 1990s as lead singer of R&B girl-group Destiny's Child. Managed by her father, Mathew Knowles, the group became one of the world's best-selling girl groups of all time. Their hiatus saw the release of Beyoncé's debut album, Dangerously in Love (2003), which established her as a solo artist worldwide, earned five Grammy Awards and featured the Billboard Hot 100 number-one singles Crazy in Love and Baby Boy"

# COMMAND ----------

question = "When did Beyonce start becoming popular?"
answer_question(model, tokenizer, question, context)
