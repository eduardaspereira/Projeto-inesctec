## notas 
- quem cria gitlab 
- fazer freeze requirements
- ran_kpms_noise_ext.json tem dados para 120 seg e os outros 10 seg por isso estão se a repetir
- sincronizar dados dos datasets: cyber-physical perception topic 
- agentes podem enviar comandos através de uma API diretamente para o OpenWrt para alterar a largura de banda, fazer roaming forçado de um UE, ou ajustar a potência do sinal rádio.



## iniciar
instalar docker: https://docs.docker.com/engine/install/ubuntu/
1. entrar no venv  
2. python src/clean_pipeline_data.py  
3. docker compose up -d na pasta config/  

 Em 3 terminais diferentes:  
4. python src/python uav_kafka_producer.py  
5. python src/cyber_physical_perception.py  
6. streamlit run dashboard.py  

docker compose restart vision-encoder

Dashboard streamlit -> http://localhost:8502/  
ver msgs em cada tópico no kafka -> http://127.0.0.1:8080/ 


docker compose down
docker compose pull
docker compose up -d 


ver topicos:
docker exec -it $(docker ps -qf "name=kafka") kafka-console-consumer --bootstrap-server localhost:29092 --topic NC_topic --from-beginning

criar topicos:
docker exec -it $(docker ps -qf "name=kafka") kafka-topics --create --topic VC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
docker exec -it $(docker ps -qf "name=kafka") kafka-topics --create --topic NC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1


consumir topico
docker exec -it config-kafka-1 kafka-console-consumer --bootstrap-server localhost:29092 --topic NC_topic --from-beginning

criar topico v2:
docker exec -it config-kafka-1 kafka-topics --create --topic VC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
docker exec -it config-kafka-1 kafka-topics --create --topic NC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

=====================================================================================================================
- Levantar o Kafka e o Zookeeper
docker-compose pull
docker-compose up -d

- Criar os Tópicos no Kafka
docker exec -it $(docker ps -qf "name=kafka") kafka-topics --create --topic VC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

docker exec -it $(docker ps -qf "name=kafka") kafka-topics --create --topic NC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

- Iniciar o Vertical Container (Consumidor Principal)
python vertical_container.py

- T2: Preparar e Iniciar o Produtor Sintético -> ver T1
python test_producer.py

- Monitorizar o Resultado Final no NC_topic
A: http://localhost:8080, vai a Topics -> NC_topic -> Messages.

B: T3
docker exec -it $(docker ps -qf "name=kafka") kafka-console-consumer --bootstrap-server localhost:29092 --topic NC_topic --from-beginning


==============================================
# Criar o VC_topic
docker exec -it cd14435d8738 kafka-topics --create --topic VC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

# Criar o NC_topic
docker exec -it cd14435d8738 kafka-topics --create --topic NC_topic --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

===============================================
Network Container:
1. Consumir a mensagem do tópico NC_topic.
2. Descomprimir o vetor: Usando a fórmula matemática inversa da quantização: Valor_{Float} = (Valor_{INT8} + 128) * scale + min_val
3. Avaliar o Espaço Latente: terá um algoritmo (provavelmente Reinforcement Learning ou um simples classificador Pytorch/SciKit) que aprendeu que, por exemplo, se a dimensão [4] do vetor for positiva e a dimensão [10] for muito negativa, isso significa "Vento Forte e Contentor em Queda".
4. Atuar na Rede: Com base nessa avaliação, o NC vai comunicar com a arquitetura O-RAN para alocar mais largura de banda (eMBB) para o vídeo ou priorizar latência (URLLC) para comandos de emergência.
O modelo preditivo do NC lê essa matemática e prevê o futuro.