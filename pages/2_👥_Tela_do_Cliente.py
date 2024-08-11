import streamlit as st
import pandas as pd
import requests, json, ast, math
from supabase import create_client, Client

st.set_page_config(layout="wide")

st.logo('assets/logo-superlogica-full-color.png')

access_token_hubspot = st.secrets["access_token_hubspot"]
app_token_assinas = st.secrets["app_token_assinas"]
access_token_assinas = st.secrets["access_token_assinas"]
url_supabase = st.secrets["url_supabase"]
key_supabase = st.secrets["key_supabase"]
api_key_zendesk = st.secrets['api_key_zendesk']

headers_hubspot = {
    'Content-Type': 'application/json',
    'Authorization': f"Bearer {access_token_hubspot}"
}

headers_assinaturas = {
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
    'app_token': f"{app_token_assinas}",
    'access_token': f"{access_token_assinas}"
}

headers_zendesk = {
    'Accept': 'application/json',
    'Authorization': f"Basic {api_key_zendesk}"
}

# ------------- FUNÇÕES

def trata_listas(string):
    if string == '[nan]':
        return []
    else:
        # Converter string para lista
        lista = ast.literal_eval(string)
        # Remover duplicações
        lista_unica = list(set(lista))
        return lista_unica

def captura_reunioes(headers_hubspot, id_empresa):
    url = f"https://api.hubapi.com/crm/v4/objects/companies/{id_empresa}/associations/meeting"
    response = requests.get(url, headers=headers_hubspot).json()
    if response['results']:
        if response['results'] == []:
            return []
        else:
            reunioes = response['results']
            lista_reunioes = []
            for reuniao in reunioes:
                lista_reunioes.append(f"{reuniao['toObjectId']}")

            url = "https://api.hubapi.com/crm/v3/objects/meetings/search"

            data = {
                "limit": 100,
                "after": None,
                "properties": ['hs_meeting_title', 'hs_object_id', 'hs_activity_type', 'hs_meeting_body', 'hs_lastmodifieddate', 'hs_created_by', 'hubspot_owner_id', 'hs_createdate'],
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "hs_object_id",
                                "operator": "IN",
                                "values": lista_reunioes
                            }
                        ]
                    }
                ]
            }

            response = requests.post(url, headers=headers_hubspot, data=json.dumps(data)).json()
            return response['results']
    else:
        return []

def captura_tarefas(headers_hubspot, id_empresa):
    url = f"https://api.hubapi.com/crm/v4/objects/companies/{id_empresa}/associations/task"
    response = requests.get(url, headers=headers_hubspot).json()
    if response['results']:
        if response['results'] == []:
            return []
        else:
            tarefas = response['results']
            lista_tarefas = []
            for tarefa in tarefas:
                lista_tarefas.append(f"{tarefa['toObjectId']}")

            url = "https://api.hubapi.com/crm/v3/objects/tasks/search"

            data = {
                "limit": 100,
                "after": None,
                "properties": ['hs_task_status', 'hs_task_subject', 'hs_createdate', 'hs_task_type','hs_task_completion_date', 'hs_lastmodifieddate', 'hs_created_by', 'hubspot_owner_id'],
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "hs_object_id",
                                "operator": "IN",
                                "values": lista_tarefas
                            }
                        ]
                    }
                ]
            }

        response = requests.post(url, headers=headers_hubspot, data=json.dumps(data)).json()
        return response['results']
    else:
        return []

def captura_negocios(headers_hubspot, id_empresa):
    url = f"https://api.hubapi.com/crm/v4/objects/companies/{id_empresa}/associations/deal"
    response = requests.get(url, headers=headers_hubspot).json()
    if response['results']:
        if response['results'] == []:
            return []
        else:
            negocios = response['results']
            lista_negocios = []
            for negocio in negocios:
                lista_negocios.append(f"{negocio['toObjectId']}")

            url = "https://api.hubapi.com/crm/v3/objects/deals/search"

            data = {
              "limit": 100,
              "after": None,
              "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "hs_object_id",
                                "operator": "IN",
                                "values": lista_negocios
                            }
                        ]
                    }
                ]
            }

            response = requests.post(url, headers=headers_hubspot, data=json.dumps(data)).json()
            return response['results']
    else:
        return []

def captura_tickets(headers_hubspot, id_empresa):
    url = f"https://api.hubapi.com/crm/v4/objects/companies/{id_empresa}/associations/ticket"
    response = requests.get(url, headers=headers_hubspot).json()
    if response['results']:
        if response['results'] == []:
            return []
        else:
            tickets = response['results']
            lista_tickets = []
            for ticket in tickets:
                lista_tickets.append(f"{ticket['toObjectId']}")

            url = "https://api.hubapi.com/crm/v3/objects/tickets/search"

            data = {
                "limit": 100,
                "after": None,
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "hs_object_id",
                                "operator": "IN",
                                "values": lista_tickets
                            }
                        ]
                    }
                ]
            }

            response = requests.post(url, headers=headers_hubspot, data=json.dumps(data)).json()
            return response['results']
    else:
        return []

def captura_tickets_por_licenca(licenca):
  url = "https://condominios.zendesk.com/api/v2/search.json"

  params = {
    'query': f"type:ticket custom_field_1500011014801:{licenca}",
  }

  all_results = []

  response = requests.request("GET", url, headers=headers_zendesk, params=params)
  returned = json.loads(response.content)
  all_results.extend(returned['results'])

  while returned['next_page'] is not None:
    url = returned['next_page']
    response = requests.request("GET", url, headers=headers_zendesk)
    returned = json.loads(response.content)
    try:
      all_results.extend(returned['results'])
    except:
      return all_results

  return all_results

# ------------- DADOS
def main_page():
    if 'cliente' in st.session_state:
        cliente = st.session_state.cliente

        st.header(cliente['st_nome_sac'], divider=True)

        tab1, tab2, tab3, tab4 = st.tabs(["Informações Gerais", "Financeiro", "Atividades CS/Comercial/Canais", "Suporte"])

        with tab1:
            st.subheader("Informações da empresa")
            st.markdown(f"**CNPJ/CPF:** {cliente['st_cgc_sac']}")
            st.markdown(f"**Endereço:** {cliente['endereco']} {cliente['st_cidade_sac']} - {cliente['st_estado_sac']}")

            ddd = cliente['st_ddd_sac']
            telefone = cliente['st_telefone_sac']

            if telefone != '':
                ddd = int(ddd)
                telefone = int(telefone)
                if telefone is not None and not math.isnan(telefone):
                    if ddd is not None and not math.isnan(ddd):
                        st.markdown(f"**Telefone principal:** ({ddd}) {telefone}")
                    else:
                        st.markdown(f"**Telefone principal:** {telefone} (sem DDD)")
                else:
                    st.markdown("**Telefone principal:** Dados indisponíveis")

            empresas_hubspot = trata_listas(cliente['hs_object_id'])
            st.markdown("##### Empresa(s) Hubspot:")
            markdown_empresas = ""
            for empresa in empresas_hubspot:
                markdown_empresas += f"- https://app.hubspot.com/contacts/20131994/record/0-2/{empresa}\n"
            st.markdown(markdown_empresas)

            planos = trata_listas(cliente['st_nome_pla'])
            licencas = trata_listas(cliente['st_identificador_plc'])
            st.markdown("##### Plano(s):")
            markdown_text = ""
            for plano in planos:
                markdown_text += f"- {plano}\n"
            st.markdown(markdown_text)

            if isinstance(cliente['cs_erp_responsavel'], str):
                cs_erp = trata_listas(cliente['cs_erp_responsavel'])
            else:
                cs_erp = []
            if isinstance(cliente['is__cs_responsavel_1'], str):
                cs_crm = trata_listas(cliente['is__cs_responsavel_1'])
            else:
                cs_crm = []

            st.markdown("##### CS responsável:")
            if cs_erp != []:
                st.markdown(f"**ERP:** {cs_erp}")
            if cs_crm != []:
                st.markdown(f"**CRM:** {cs_crm}")

            st.subheader("Pessoas: contatos do cliente no Hubspot")

            contatos = st.session_state.contatos
            # Lista de colunas desejadas
            colunas_desejadas = ['firstname', 'lastname', 'email', 'phone', 'telefone']
            # Verificar se as colunas desejadas estão presentes no DataFrame
            colunas_existentes = [col for col in colunas_desejadas if col in contatos.columns]
            if colunas_existentes:
                # Filtrar o DataFrame com as colunas existentes
                contatos_filtrados = contatos[colunas_existentes]
                st.dataframe(contatos_filtrados, use_container_width=True)

        with tab2:
            st.subheader("Informações do cliente no Assinaturas")
            st.dataframe(cliente, use_container_width=True)

            st.subheader("Produtos do cliente no Assinaturas")
            filtro_status_produto = st.multiselect("Filtre por status", options=st.session_state.produtos['status'].unique(),default=st.session_state.produtos['status'].unique())
            produtos = st.session_state.produtos
            df_filtrado_produtos = produtos[(produtos['status'].isin(filtro_status_produto))]
            st.dataframe(df_filtrado_produtos, use_container_width=True)

            st.subheader("Últimas cobranças do cliente no Assinaturas")
            filtro_status_cobranca = st.multiselect("Filtre por status", options=st.session_state.cobrancas['fl_status_recb'].unique(),default=st.session_state.cobrancas['fl_status_recb'].unique())
            cobrancas = st.session_state.cobrancas
            df_filtrado_cobrancas = cobrancas[(cobrancas['fl_status_recb'].isin(filtro_status_cobranca))]
            st.dataframe(df_filtrado_cobrancas, use_container_width=True)

        with tab3:
            if cliente['hs_object_id'] != '[nan]':
                lista_hubspot = trata_listas(cliente['hs_object_id'])
                lista_reunioes = []
                lista_tarefas = []
                lista_negocios = []
                lista_tickets = []
                with st.spinner('Consultando dados'):
                    for empresa in lista_hubspot:
                        lista_reunioes.extend(captura_reunioes(headers_hubspot, empresa))
                        lista_tarefas.extend(captura_tarefas(headers_hubspot, empresa))
                        lista_negocios.extend(captura_negocios(headers_hubspot, empresa))
                        lista_tickets.extend(captura_tickets(headers_hubspot, empresa))
                    reunioes = [item['properties'] for item in lista_reunioes]
                    tarefas = [item['properties'] for item in lista_tarefas]
                    negocios = [item['properties'] for item in lista_negocios]
                    tickets = [item['properties'] for item in lista_tickets]
                    reunioes = pd.DataFrame(reunioes)
                    tarefas = pd.DataFrame(tarefas)
                    tarefas = tarefas[['hs_createdate', 'hs_task_completion_date', 'hs_task_subject', 'hs_task_status', 'hubspot_owner_id', 'hs_object_id', 'hs_task_type', 'hs_lastmodifieddate', 'hs_created_by']]
                    colunas_datas = ['hs_createdate', 'hs_task_completion_date', 'hs_lastmodifieddate']
                    tarefas[colunas_datas] = tarefas[colunas_datas].apply(pd.to_datetime, errors='coerce')
                    tarefas[colunas_datas] = tarefas[colunas_datas].apply(lambda x: x.dt.strftime('%d/%m/%Y'))
                    negocios = pd.DataFrame(negocios)
                    tickets_hubspot = pd.DataFrame(tickets)
                    st.subheader("Reuniões registradas no Hubspot")
                    st.dataframe(reunioes, use_container_width=True)
                    st.subheader("Tarefas registradas no Hubspot")
                    filtro_status_tarefa = st.multiselect("Filtre por status", options=tarefas['hs_task_status'].unique(), default=tarefas['hs_task_status'].unique())
                    df_filtrado_tarefas = tarefas[(tarefas['hs_task_status'].isin(filtro_status_tarefa))]
                    st.dataframe(df_filtrado_tarefas, use_container_width=True)
                    st.subheader("Negócios registrados no Hubspot")
                    st.dataframe(negocios, use_container_width=True)
            else:
                st.write('Empresa não encontrada no Hubspot')

        with tab4:
            licencas = trata_listas(cliente['st_identificador_plc'])
            lista_tickets = []
            with st.spinner('Consultando dados do Zendesk'):
                for licenca in licencas:
                    lista_tickets.extend(captura_tickets_por_licenca(licenca))
                simplified_tickets = []
                for ticket in lista_tickets:
                    simplified_ticket = {
                        'url': ticket['url'],
                        'id': ticket['id'],
                        'via': ticket['via'],
                        'created_at': ticket['created_at'],
                        'updated_at': ticket['updated_at'],
                        'type': ticket['type'],
                        'subject': ticket['subject'],
                        'description': ticket['description'],
                        'priority': ticket['priority'],
                        'status': ticket['status']
                    }
                    simplified_tickets.append(simplified_ticket)
                tickets = pd.DataFrame(simplified_tickets)
                st.subheader("Tickets com a licença registrada no Zendesk")
                st.dataframe(tickets, use_container_width=True)

                st.subheader("Tickets registrados no Hubspot")
                if cliente['hs_object_id'] != '[nan]':
                    st.dataframe(tickets_hubspot, use_container_width=True)

    else:
        st.write("Nenhum cliente selecionado")

def login(email, password):
    try:
        # Sign in with Supabase
        user = supabase.auth.sign_in_with_password({
            'email': email,
            'password': password,
        })
        return user
    except Exception as e:
        st.error(f"Error: {e}")
        return None

def login_page():
    st.subheader("Login")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = login(email, password)
        if user:
            st.success("Login realizado com sucesso!")
            st.session_state["authenticated"] = True
            st.session_state["show_login_modal"] = False
            st.rerun()
        else:
            st.error("Erro no login. Verifique suas credenciais.")


def main():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"] == True:
        main_page()
    else:
        login_page()

main()