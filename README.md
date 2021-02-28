# Advanced Information Retrieval: ChatonIR 

Diese Arbeit ist im für das Modul Advanced Information Retrieval entstanden und behandelt das Problem des scientific document retrieval.

### Benötige Ressourcen 

Es wird eine conda environment erstellt mittels:

```conda env create```

Anschließend wird die enviroment geupdated:

```conda env update --prune```

und aktiviert um unser Programm starten zu können:

```conda activate chatonir_finn_env```

### Nutzung

- Das Script index.py erstellt den Elasticsearch Index und Indeziert alle Daten
- Das Script main.py startet eine Suchmaschinenanfrage und kann über die Kommandozeile bedient werden

### Laufzeit

Das Script index.py wird automatische den ganzen Corpus der DBLP Daten indezieren. Da hierfür die Keyqueries vorberechnet
werden kann diese einige Zeit dauern. Falls die Suchmaschine auf einem kleineren Datensatz ausprobiert werden soll, muss dieser 
bei ``` se.index_data() ``` in index.py geändert werden. 
