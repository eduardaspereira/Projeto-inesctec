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


Dashboard streamlit -> http://localhost:8502/  
ver msgs em cada tópico no kafka -> http://127.0.0.1:8080/ 


