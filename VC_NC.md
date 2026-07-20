# O que o VC TEM de fazer
Extração Multimodal Paralela: Processar a imagem via CLIP (512 dims), o texto TOS via MiniLM (128 dims) e a telemetria via IMU Autoencoder (128 dims).

Agregação Dinâmica do IMU: Respeitar rigidamente a janela temporal de 50 amostras e extrair as 12 características estatísticas (médias, desvios-padrão, mínimos e máximos) antes de passar pela rede.

Fusão e Redução Dimensional: Concatenar os três vetores num vetor de 768 dimensões e aplicar o modelo PCA para o reduzir para exatamente 256 dimensões.

Quantização INT8: Converter os valores flutuantes resultantes do PCA para números inteiros entre -128 e 127, capturando obrigatoriamente os valores scale e min_val.

Privacidade e Largura de Banda: Nunca enviar imagens (.png), textos em bruto ou telemetria nativa para o Kafka. Apenas o vetor quantizado e os parâmetros podem viajar na rede.

Tolerância a Falhas: Garantir que, se a câmara falhar ou não houver evento TOS, o sistema injeta um array de zeros (e não um erro fatal), mantendo o fluxo de dados constante.

# O que eleva o VC a Estado-da-Arte
Exportação Contínua para TensorBoard: Manter a gravação em segundo plano dos ficheiros tensors.tsv e metadata.tsv para permitir auditorias visuais e provar ao júri/professores a separação de clusters do espaço latente.

Aviso Prévio de Estado: Adicionar uma flag simples no JSON final (ex: "status": "CRITICAL_PROXIMITY") caso o VC detete localmente anomalias gigantescas nos sensores, alertando o NC antes mesmo da descodificação.

Limpeza do Buffer: Garantir que o buffer circular (deque) do IMU é limpo corretamente caso haja uma quebra longa de ligação (para não misturar dados de voos diferentes).

# O que o NC TEM de fazer
Consumo Contínuo: Estar permanentemente subscrito ao NC_topic do Kafka e ler as mensagens à velocidade a que chegam.

Descompressão Matemática Exata: Reverter obrigatoriamente a quantização usando a fórmula algébrica inversa para recuperar a precisão decimal

Acumulação Histórica: O NC não pode analisar apenas a mensagem atual isolada. Tem de manter uma janela de histórico (ex: os últimos 10 ou 20 vetores) na memória para ter noção de tempo e velocidade.

Decisão de Slicing da Rede: O algoritmo final do NC tem de culminar numa decisão explícita para a infraestrutura de rede (ex: pedir uma fatia de rede URLLC para latência ultra-baixa se prever uma colisão, ou eMBB para largura de banda se for um voo normal).

# O que eleva o NC a Estado-da-Arte
Previsão Sequencial (LSTM ou GRU): Em vez de usar regras fixas (IF/ELSE), treinar uma pequena rede neural recorrente (LSTM) que leia os últimos 5 segundos de vetores e consiga prever o vetor do segundo seguinte.

Detenção Proativa de Bloqueio de LoS: usar a trajetória no espaço latente para prever matematicamente que o obstáculo (contentor) vai intersetar a Linha de Visada (LoS) entre a antena (AP) e o recetor (UE) antes que o sinal degrade fisicamente.

Métricas de Desempenho: Medir a latência exata (em milissegundos) desde o timestamp em que tu geraste a mensagem no UAV até ao momento em que o NC tomou a decisão de rede. Isto prova a viabilidade da arquitetura na vida real.

Não pode haver mock data. Não usar emojis.
usa pkl antigo?
pkl está a ser publicado no NC_topic?
treinar rede neural no colab
o test_producer.py está a enviar alternadamente para  os tópicos ou está a enviar por ordem? 



prever qnt tempo ao ritmo 