# TF Estimación de la Demanda Eléctrica Mensual de Argentina
Este repo presenta una solución desarrollada en el marco del Trabajo Final de la Especialización en Ciencia de Datos del ITBA, cursada entre 2021 y 2022.

Creado por C.Olmo colmo@itba.edu.ar, colmo786@gmail.com

## Objetivo de la Solución
Presentar, con frecuencia mensual, la proyección de la demanda de energía eléctrica de Argentina para los próximos 6 meses, según los pronósticos de diferentes modelos de aprendizaje automático.

## Conceptos de la Implementación de la Solución
La solución se implementó en una código Python, aon Airflow como robot de ejecución de las tareas rutinarias y persistiendo la data en una base Postgres local. Se desarrolló en una máquina Windows 10 con 16 Gb de ram. Para poder ejecutar Airflow local se instaló WSL. Como IDE se utilizó VS Code.
La solución tiene como tareas macro:
- [Airflow Automatización] Se obtienen en forma diaria el dato de demanda mensual de energía eléctrica, a nivel total Argentina. Este dato es publicado por la empresa Cammesa. Si bien el dato es mensual, de antemano no se conoce la fecha de publicación, con lo cual, el robot consulta todos los días. 
- [Postgres] Se registran estos datos en una base de datos donde se mantiene la historia. Modelo de Datos preparado para expandir la solución incorporando más variables en la estimación.
- [Python] Con nuevos datos obtenidos se re entrenan 10 modelos para obtener una predicción de los próximos 6 meses:
    - Serie compuesta por la suma de un polígono + una función senoidal que simulen la tendencia y la seasonability.
    - ARIMA
    - Prophet
    - Una red neuronal simple (feed forward) que toma como input 12 meses y presenta como output la estimación del mes 13.
    - Una red neuronal time lagged, que también utiliza como input los 12 meses anteriores más los valores del mismo mes (ej: todos los marzos) de los últimos 6 años.
    - Una red neuronal LSTM vainilla y stacked.
    - Convolutional neuron network.
    - SVM.
- [Postgres] Se persisten los resultados de cada experimento/pronóstico. Se mantiene la historia de pronósticos por modelo.
- [Power Bi] Se publican los pronósticos e indicadores de performance de cada modelo, así como gráficos que buscan ayudar a la selección del mejor pronóstico cada mes.

[Documento Entregable del Trabajo - EN ELABORACIÓN](.\doc\TF OLMO - Draft.pdf)