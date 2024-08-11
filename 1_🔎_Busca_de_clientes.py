import streamlit as st
import pandas as pd
import ast, requests, json
from supabase import create_client, Client

st.set_page_config(layout="wide")

st.logo('assets/logo-superlogica-full-color.png')

access_token_hubspot = st.secrets["access_token_hubspot"]
app_token_assinas = st.secrets["app_token_assinas"]
access_token_assinas = st.secrets["access_token_assinas"]
url_supabase = st.secrets["url_supabase"]
key_supabase = st.secrets["key_supabase"]

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

# Initialize Supabase client
supabase: Client = create_client(url_supabase, key_supabase)

def buscar_no_dataframe(df, criterio):
    resultado = df[
        df.apply(lambda row: row.astype(str).str.contains(criterio, case=False).any(), axis=1)
    ]
    return resultado

@st.cache_resource
def init_connection():
    url = url_supabase
    key = key_supabase
    return create_client(url, key)

supabase = init_connection()

@st.cache_resource
def run_query():
    data = []
    start = 0
    limit = 1000
    while True:
        response = supabase.table("clientes-sl").select("*").range(start, start + limit - 1).execute()
        if not response.data:
            break
        data.extend(response.data)
        start += limit
    return data

rows = run_query()
if rows:
    banco = pd.DataFrame(rows)

def trata_listas(string):
    if string == '[nan]':
        return []
    else:
        # Converter string para lista
        lista = ast.literal_eval(string)
        # Remover duplicações
        lista_unica = list(set(lista))
        return lista_unica

def trata_cobrancas(lista):
    def statusrecb(status):
        if status == '0':
            return 'Aberto'
        elif status == '1':
            return 'Pago'

    cobrancas = pd.DataFrame(lista)
    cobrancas = cobrancas[
        ['st_email_sac', 'dt_vencimento_recb', 'dt_recebimento_recb', 'nome_forma_pagamento_cliente',
         'fl_status_recb', 'vl_total_recb', 'vl_emitido_recb', 'link_2via', 'nota']]
    colunas_datas = ['dt_vencimento_recb', 'dt_recebimento_recb']
    colunas_valor = ['vl_total_recb', 'vl_emitido_recb']
    cobrancas[colunas_datas] = cobrancas[colunas_datas].apply(pd.to_datetime, errors='coerce')
    cobrancas[colunas_valor] = cobrancas[colunas_valor].apply(pd.to_numeric, errors='coerce')
    cobrancas['fl_status_recb'] = cobrancas['fl_status_recb'].apply(statusrecb)
    cobrancas = cobrancas.sort_values(by='dt_vencimento_recb', ascending=False).head(10)

    return cobrancas

def captura_produtos(id_sacado, headers):
    url = f"https://api.superlogica.net/v2/financeiro/assinaturas?ID_SACADO_SAC={id_sacado}&pagina=1&itensPorPagina=50&comContrato=&avulsas="
    response = requests.get(url, headers=headers).json()
    assinaturas = response[0]['data']
    produtos = []

    for assinatura in assinaturas:
        assin = {}

        assin['licença'] = assinatura['st_identificador_plc']
        assin['nome do plano'] = assinatura['st_nome_pla']
        assin['categoria do plano'] = assinatura['st_nome_gpl']
        assin['data de inicio'] = assinatura['dt_contrato_plc']
        if assinatura['dt_cancelamento_plc'] == '':
            assin['status'] = 'ativo'
        else:
            assin['status'] = 'inativo'
            assin['cancelamento'] = assinatura['dt_cancelamento_plc']
        if assinatura['fl_periodicidade_pla'] == '1':
            assin['periodicidade'] = 'anual'
        else:
            assin['periodicidade'] = 'mensal'

        for produto in assinatura['mensalidade']:
            descricao = produto.get('st_descricao_prd', '').lower()
            valor_mens = produto.get('st_valor_mens')

            if 'taxa de licenciamento' in descricao:
                assin['produto'] = "ERP"
                assin['mensalidade'] = valor_mens
            elif 'owli' in descricao:
                assin['produto'] = "Owli"
                assin['mensalidade'] = valor_mens
            elif 'crm cobranças' in descricao:
                assin['produto'] = "CRM Cobranças"
                assin['mensalidade'] = valor_mens
            elif 'descontos' in descricao:
                assin['desconto'] = valor_mens

        produtos.append(assin)
    return produtos

def captura_contatos(id_empresa, headers_hubspot):
    url = f"https://api.hubapi.com/crm/v4/objects/companies/{id_empresa}/associations/contact"
    response = requests.get(url, headers=headers_hubspot).json()
    contatos = response['results']
    lista_contatos = []
    for contato in contatos:
        lista_contatos.append(f"{contato['toObjectId']}")

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"

    data = {
        "limit": 100,
        "after": None,
        "properties": ['hs_role', 'firstname', 'lastname', 'email', 'telefone', 'telefone_de_contato', 'time', 'hs_whatsapp_phone_number', 'phone'],
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_object_id",
                        "operator": "IN",
                        "values": lista_contatos
                    }]
            }
        ]
    }

    response = requests.post(url, headers=headers_hubspot, data=json.dumps(data)).json()
    return response['results']

def captura_cobrancas(id_sacado, headers_assinas):
    url = f"https://api.superlogica.net/v2/financeiro/cobranca?doClienteComId={id_sacado}&pagina=1&itensPorPagina=50"
    cobrancas = requests.get(url, headers=headers_assinas).json()

    return cobrancas

def buscar_no_dataframe(df, criterio):
    resultado = df[
        df.apply(lambda row: row.astype(str).str.contains(criterio, case=False).any(), axis=1)
    ]
    return resultado


# Função para exibir detalhes do cliente
@st.dialog("Detalhes do cliente", width="large")
def consulta_cliente(index):
    cliente = banco.iloc[index]
    st.subheader("Dados gerais 🎲", divider=True)
    st.write(cliente)

    with st.spinner('Consultando dados'):
        ids_assinaturas = trata_listas(cliente['id_sacado_sac'])
        ids_hubspot = trata_listas(cliente['hs_object_id'])
        lista_produtos = []
        lista_cobrancas = []

        for id in ids_assinaturas:
            produtos = captura_produtos(id, headers_assinaturas)
            cobrancas = captura_cobrancas(id, headers_assinaturas)
            lista_produtos.extend(produtos)
            lista_cobrancas.extend(cobrancas)

        produtos = pd.DataFrame(lista_produtos)
        st.subheader('Produtos SaaS 🖥️', divider=True)
        filtro_status_produto = st.multiselect("Filtre por status", options=produtos['status'].unique(), default=produtos['status'].unique())
        st.write(produtos)
        lista_contatos = []

        for id in ids_hubspot:
            contatos = captura_contatos(id, headers_hubspot)
            lista_contatos.extend(contatos)

        contatos = [contato['properties'] for contato in lista_contatos]
        contatos = pd.DataFrame(contatos)
        st.subheader('Pessoas 👥', divider=True)
        with st.expander('Ver pessoas'):
            st.write(contatos)

        cobrancas = trata_cobrancas(lista_cobrancas)
        st.subheader('Cobranças 💰', divider=True)
        with st.expander('Ver últimas cobranças'):
            filtro_status_cobranca = st.multiselect("Filtre por status", options=cobrancas['fl_status_recb'].unique(), default=cobrancas['fl_status_recb'].unique())
            st.write(cobrancas)


    if st.button("Ir para página do cliente"):
        st.session_state.cliente = cliente
        st.session_state.contatos = contatos
        st.session_state.produtos = produtos
        st.session_state.cobrancas = cobrancas
        st.switch_page("pages/2_👥_Tela_do_Cliente.py")

def main_page():
    # Inicializando session state para armazenar o índice do cliente
    if 'cliente_index' not in st.session_state:
        st.session_state.cliente_index = None

    # Formulário de busca
    with st.form(key='formulario'):
        st.header("Busca de clientes")
        st.write("Busque seu cliente por Razão Social, Nome Fantasia, Licença, CNPJ (apenas números), E-mail (apenas números). É possível encontrar as informações com apenas parte da palavra na busca.")
        busca = st.text_input("Busque seu cliente:")
        botao = st.form_submit_button(label="Buscar")

    if botao:
        # Realizar a busca e armazenar resultados no session state
        st.session_state.cliente_index = None
        st.session_state.resultado = buscar_no_dataframe(banco, busca)

    if 'resultado' in st.session_state:
        resultado = st.session_state.resultado
        st.header('Resultado da busca 🔎', divider=True)

        for index, row in resultado.iterrows():
             st.subheader(f"Razão social: {row['st_nome_sac']}")
             st.markdown(f"Licença(s): {row['st_identificador_plc']}")
             st.write(f"Plano(s): {row['st_nome_pla']}")

             if st.button("ver mais", key=f"detalhes_{index}"):
                st.session_state.cliente_index = None
                st.session_state.cliente_index = index

             st.divider()

    # Mostrar detalhes do cliente se um botão foi clicado
    if st.session_state.cliente_index is not None:
        consulta_cliente(st.session_state.cliente_index)

### ---------------------------- CORRIGIR DAQUI PARA BAIXO

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