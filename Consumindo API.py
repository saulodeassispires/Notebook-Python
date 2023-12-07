##from typing import Concatenate


def consulta_cep():
    ##### importa biblioteca para chamada api ( instalado pelo pip)
    import requests
    print("consulta ceps")

    cep_input = input("digite o cep para consulta : ")


    # verifica tamanho do cep digitado se estiver ok chama API senao aborta 
    if len(cep_input) != 8: 
        print("quantidade de digitos invalida")
        exit()
    else:
        request = requests.get('https://viacep.com.br/ws/{}/json/'.format(cep_input)) 

    #print(request.json())
    address_data = request.json()
    
    if 'erro' not in address_data:
        #print("cep correto")
        print('\n\n')
        print('cep {}'.format(address_data['cep']))
        print('logradouro {}'.format(address_data['logradouro']))
        print('bairro {}'.format(address_data['bairro']))
        print('cidade {}'.format(address_data['localidade']))
        print('\n\n')
        bairro = address_data['bairro']
        texto  = 'teste'
        ##print(Concatenate([texto,bairro]))
        ##print(address_data)
    else:
        print("cep invalido {}".format(cep_input))

#if qtd == 0:
#   escolha = input('Deseja efetuar outra consulta ? \n')
#   exit()

consulta_cep()




