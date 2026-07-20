import csv

# =====================================================================
# CONFIGURACAO DO INTERVALO DE BLOQUEIO (Line of Sight)
# Substitui estes valores pelas linhas exatas onde a obstrucao ocorre
# =====================================================================
INICIO_BLOQUEIO = 126  # Linha onde o contentor comeca a tapar o sinal
FIM_BLOQUEIO = 142     # Linha onde o contentor sai da frente do sinal

tensors_entrada = 'tensors.tsv'
tensors_saida = 'tensors_with_labels.tsv'

print("A iniciar o processo de anotacao do dataset espacial (Ground Truth)...")

linhas_processadas = 0
linhas_bloqueadas = 0

with open(tensors_entrada, 'r') as f_in, open(tensors_saida, 'w', newline='') as f_out:
    leitor = csv.reader(f_in, delimiter='\t')
    escritor = csv.writer(f_out, delimiter='\t')
    
    for index, linha in enumerate(leitor):
        if not linha:
            continue
            
        # Atribui a label 1 (Bloqueio) estritamente dentro do intervalo de obstrucao fisica
        if INICIO_BLOQUEIO <= index <= FIM_BLOQUEIO:
            label_los = 1
            linhas_bloqueadas += 1
        else:
            label_los = 0
            
        linha.append(str(label_los))
        escritor.writerow(linha)
        linhas_processadas += 1

print(f"Anotacao concluida com sucesso. Ficheiro gerado: '{tensors_saida}'.")
print(f"Total de vetores processados: {linhas_processadas}")
print(f"Vetores classificados como Bloqueio LoS (Classe 1): {linhas_bloqueadas}")