import pandas as pd
import numpy as np
import os
from openpyxl import Workbook


#acessando aos arquivos broker e executando os processos ETL para ter arquivos mais confiaveis.

cam_broker = 'C:/Users/Gregory Toledo/Conferencia-saldo-entre-sistemas/broker'
#pasta com os arquivos broker
arq_atual = os.listdir(cam_broker)

#Listas criadas para recolher os valores do arquivo do broker
processo_brok = []
cliente_brok = []
cnpj_brok = []
valor_brok = []
processo_brok1 = []
cliente_brok1 = []
cnpj_brok1 = []
valor_brok1 = []
#for utilizado para acessar cada arquivo diariamente
for cont1 in range(len(arq_atual)):
    arq_broker = pd.read_excel(cam_broker +'/'+ arq_atual[cont1])
    #for usado para adicionar as colunas dentro das listas
    for cont2 in range(len(arq_broker)):
        processo_brok.append(str(arq_broker['Unnamed: 1'].loc[cont2]))
        cliente_brok.append(str(arq_broker['Unnamed: 2'].loc[cont2]))
        cnpj_brok.append(str(arq_broker['Unnamed: 35'].loc[cont2]))
        #alguns arquivos o valor está em 41 ou 36, por isso o if abaixo
        if 'Unnamed: 41' in arq_broker.columns:
            valor_brok.append(str(arq_broker['Unnamed: 41'].loc[cont2]))
        else:
            valor_brok.append(str(arq_broker['Unnamed: 36'].loc[cont2]))


#for utilizado para remover as linhas inuteis abaixo, pois não referenciam aos processos            
for cont3 in range(len(cliente_brok)):
    if cliente_brok[cont3] == 'Processo':
        cliente_brok[cont3] = 'nan'
    if cliente_brok[cont3] == 'Total Cliente:':
        cliente_brok[cont3] = 'nan'
        valor_brok[cont3] = 'nan'
    if cliente_brok[cont3] == 'Total:':
        cliente_brok[cont3] = 'nan'
        

#for utilizado para remover os nan dentro das listas abaixo      
for cont4 in range(len(cliente_brok)):
    if cont4 ==0:
        cont4 = 1
    if cliente_brok[cont4] == 'nan':
        cliente_brok[cont4] = cliente_brok[cont4-1]
        cnpj_brok[cont4] = cnpj_brok[cont4-1]
    if processo_brok[cont4] == 'nan':
        processo_brok[cont4] = processo_brok[cont4-1]
        processo_brok[cont4] = processo_brok[cont4-1]
    else:
        pass
    
#utilizando as outras listas para trazer os processos sem duplicidade
for cont5 in range(len(valor_brok)):
    if valor_brok[cont5] !='nan':
        processo_brok1.append(processo_brok[cont5])
        cliente_brok1.append(cliente_brok[cont5])
        cnpj_brok1.append(cnpj_brok[cont5])
        valor_brok1.append(valor_brok[cont5])
        
#Criando lista com dicionarios, com as informações do broker
lista_broker = []
for contad in range(len(processo_brok1)):
    brok = {}
    brok['PROCESSO'] = processo_brok1[contad]
    brok['CLIENTE'] = cliente_brok1[contad]
    brok['CNPJ'] = cnpj_brok1[contad]
    brok['VALOR'] = valor_brok1[contad]
    lista_broker.append(brok)

#ajustando os valores, para que fiquem corretamente de acordo com o tipo float
contad = 0
for contad in range(len(lista_broker)):
    lista_broker[contad]['VALOR'] = lista_broker[contad]['VALOR'].replace('.','')
    lista_broker[contad]['VALOR'] = lista_broker[contad]['VALOR'].replace(',','.')
    lista_broker[contad]['VALOR'] = float(lista_broker[contad]['VALOR'])

#SAP:

processo_sap = []
codigo_sap = []
saldo_sap = []
proc_sap = []
processo_sap1 = []
codigo_sap1 = []
saldo_sap1 = []
proc_sap1 = []
cam_sap = 'C:/Users/Gregory Toledo/Conferencia-saldo-entre-sistemas/sap'
#lista dos arquivos do SAP
arq_atual_SAP = os.listdir(cam_sap)
for cont6 in range(len(arq_atual_SAP)):
    arq_sap = pd.read_excel(cam_sap +'/'+ arq_atual_SAP[cont6])
    #for usado para adicionar as colunas dentro das listas, o script é capaz de ler tanto os arquivos em ingles do sap quanto os arquivos em portugues
    for cont7 in range(len(arq_sap)):
        if 'Observações' in arq_sap.columns: 
            processo_sap.append(arq_sap['Observações'].loc[cont7])
            codigo_sap.append(arq_sap['Data de vencimento'].loc[cont7])
            saldo_sap.append(arq_sap['Débito/crédito (MC)'].loc[cont7])
            proc_sap.append(arq_sap['Data de lançamento'].loc[cont7])
        elif 'Remarks' in arq_sap.columns:
            processo_sap.append(arq_sap['Remarks'].loc[cont7])
            codigo_sap.append(arq_sap['Due Date'].loc[cont7])
            saldo_sap.append(arq_sap['Deb./Cred. (LC)'].loc[cont7])
            proc_sap.append(arq_sap['Posting Date'].loc[cont7])
            
#incluindo o codigo do sap do cliente em todos os seus processos para melhor identificação
for cont8 in range(len(codigo_sap)):
    if cont8 ==0:
        pass
    else:
        if (codigo_sap[cont8] =='Project Code') or (codigo_sap[cont8] =='Código do projeto'):
            codigo_sap[cont8] = codigo_sap[cont8-1]

#for utilizado para inserir nas listas finais somente os processos com saldo diferente de 0, sendo os saldos pendentes no sap para conferir com o broker de processos abertos
for cont9 in range(len(saldo_sap)):
    if proc_sap[cont9] =='Total':
        if (saldo_sap[cont9]!=0) & (saldo_sap[cont9]!=np.nan)  :
            processo_sap1.append(processo_sap[cont9])
            proc_sap1.append(proc_sap[cont9])
            saldo_sap1.append(saldo_sap[cont9])
            codigo_sap1.append(codigo_sap[cont9])
#json do SAP com todos os processos abertos recendo as listas finais
lista_sap = []
for contad1 in range(len(processo_sap1)):
    sap = {}
    sap['PROCESSO'] = processo_sap1[contad1]
    sap['SALDO'] = saldo_sap1[contad1]
    sap['CODIGO SAP'] = codigo_sap1[contad1]        
    lista_sap.append(sap)


# criando o dataframe definitivo que reune todas as informações, unindo em uma lista só e removendo as duplicatas dos processos do broker e do sap    
pr = processo_brok1 + processo_sap1
allprocess= list(set(pr))    
defin = pd.DataFrame(index = range(len(allprocess)), columns=['PROCESSO','CLIENTE','CNPJ','CODIGO SAP','SALDO BROKER','SALDO SAP','DIFERENÇA'])
defin['PROCESSO'] = allprocess
for cont19 in range(len(defin)):
    defin['PROCESSO'].loc[cont19] = str(defin['PROCESSO'].loc[cont19])
    defin['PROCESSO'].loc[cont19] = defin['PROCESSO'].loc[cont19].strip(" ")



#inserindo as informações do broker no dataframe definitivo
for cont12 in range(len(defin)):
    for cont13 in range(len(lista_broker)):
        if defin['PROCESSO'].loc[cont12] == lista_broker[cont13]['PROCESSO'].strip(" "):#as vezes alguns processos contem espaço, que atrapalha na conferencia
            defin['CLIENTE'].loc[cont12] = lista_broker[cont13]['CLIENTE']
            defin['CNPJ'].loc[cont12] = lista_broker[cont13]['CNPJ']
            defin['SALDO BROKER'].loc[cont12] = lista_broker[cont13]['VALOR']
#inserindo as informações do sap no dataframe definitivo            
for cont14 in range(len(defin)):
    for cont15 in range(len(lista_sap)):
        lista_sap[cont15]['PROCESSO'] = str(lista_sap[cont15]['PROCESSO'])
        if defin['PROCESSO'].loc[cont14] == lista_sap[cont15]['PROCESSO'].strip(" "):
            defin['SALDO SAP'].loc[cont14] = lista_sap[cont15]['SALDO']
            defin['CODIGO SAP'].loc[cont14] = lista_sap[cont15]['CODIGO SAP']



#substituindo nan por 0 para não atrapalhar a operação

defin['SALDO BROKER'] = defin['SALDO BROKER'].replace(np.nan, 0.00, regex=True)
defin['SALDO SAP'] = defin['SALDO SAP'].replace(np.nan, 0.00, regex=True)



#ordenando pelo codigo do SAP
defin = defin.sort_values(by='CODIGO SAP')
#criando a tabela de excel com o dataframe final.
defin.to_excel('C:/Users/Gregory Toledo/Conferencia-saldo-entre-sistemas/Saldo.xlsx',engine = 'openpyxl',encoding='utf-8',index=False)