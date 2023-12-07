tipo = 'texto'
request = ''

def consulta_api_local():
    
    import requests
    tipo = input("Digite o caminho get ")

    if tipo not in ( 'tudo', 'country', 'all', ''):
       print(f'Erro path invalido {tipo}')
    else:
       request = requests.get(f'http://127.0.0.1:8080/{tipo}') 


    address_data = request.json()
    if tipo == 'all':
      print('Modules{}'.format(address_data['Modules']))    
      print('subject{}'.format(address_data['Subject']))
      print('Support{}'.format(address_data['Support']))
    else:
      print('Modules{}'.format(address_data['Modules']))    
      print('subject{}'.format(address_data['Subject']))

        

consulta_api_local()




