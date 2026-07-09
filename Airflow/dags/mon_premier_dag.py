from airflow.operators.python import PythonOperator  
from airflow import DAG
from datetime import datetime

def starting():    #1ère fonction
    print("Démarrage")

def processing():  #2ème fonction
    print("Traitement...")

def ending():      #3ième fonction
    print("Fin")
    
with DAG("mon_premier_dag", start_date=datetime(2024,1,1), schedule_interval="@daily",catchup=False) as dag:

   t1 = PythonOperator(            #task 1
       task_id="starting",
       python_callable=starting
   ) 

   t2 = PythonOperator(            #task 2
       task_id="processing",
       python_callable=processing
   )

   t3 = PythonOperator(            #task 3
       task_id="ending",
       python_callable=ending
   )

t1>>t2>>t3 #ordre d'exécution