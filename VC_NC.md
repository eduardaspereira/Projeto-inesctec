1. Image Perception Agent (Vision/Camera)
Implementado:
- Split-Computing at the Edge: O modelo CLIP processa tudo localmente no vertical_container.py e extrai o vetor de 512 dimensões. Em nenhuma parte do código enviamos as imagens brutas (.png) para o Kafka; enviamos apenas a matriz quantizada ($\lambda_{t}=z(\zeta_{t})$).  

Sugestões: 
- Illumination-Adaptive Modulation: simulação nevoeiro ou noite
- Safety-Critical Region Masking: O CLIP analisa a imagem inteira. Como otimização futura, podemos passar ao CLIP apenas o "recorte" da imagem (o crop do contentor) em vez da imagem completa de 500x500. Isso pouparia imensos ciclos de CPU/GPU na Edge.

2. NLP Perception Agent (TOS Synthetic Events)
Implementado:
- usamos all-MiniLM-L6-v2, a recomendação da literatura para Edge Computing.
- Contrastive Dimensionality Reduction: A função de fusão $\mathcal{F}$ exige um alinhamento rigoroso. Tu utilizas uma camada de projeção linear (nn.Linear(384, 128)) para esmagar a saída do MiniLM e não ofuscar o Autoencoder do IMU (também de 128 dimensões).  

Sugestões:
- Contrastive Dimensionality Reduction: ideal para treinar a projeção linear, mas a abordagem atual já garante o alinhamento matricial para as 768 dimensões totais.  

3. The Fusion Core: Tying the Agents Together
Implementado:
- Asynchronous N-ODE Propagation (StreamingFlow): Usamos a abordagem Last-Known-State. Quando os dados da câmara (1Hz) demoram a chegar, o código usa a última posição conhecida enquanto o IMU (a frequências mais altas) continua a atualizar.  


Sugestões:
- Asynchronous N-ODE Propagation (StreamingFlow): As Neural Ordinary Differential Equations (N-ODEs) são a melhor forma matemática de modelar o tempo contínuo com chegadas irregulares de dados. No entanto, exigem uma reestruturação complexa da rede neuronal preditiva. Para a escala do projeto, o deque histórico e o bloqueio assíncrono do Kafka cumprem o requisito sem entrarem em complexidade exagerada.

- Channel-Adaptive Semantic Gating (CASM): se o parâmetro sinr_db (Signal-to-Interference-plus-Noise Ratio) ou bler (Block Error Rate) recebido indicar que a rede sem fios está caótica, o Network Controller (NC) deve desconfiar do vetor de perceção $\lambda_{t}$ que lhe chega. Matematicamente, aplica-se uma "gate" à entrada da rede LSTM, reduzindo o peso da observação se a ligação estiver altamente degradada.  
