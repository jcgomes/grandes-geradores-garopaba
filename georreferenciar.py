from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import sqlite3
import time

# Configurar o geocodificador com rate limiting
geolocator = Nominatim(user_agent="cnpj_geocodificacao")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

def geocodificar_enderecos():
    """
    Busca endereços do banco de dados, geocodifica e salva as coordenadas WKT
    """
    # Conectar ao banco de dados
    conn = sqlite3.connect('cnpj_receita.db')
    cursor = conn.cursor()
    
    try:
        # Buscar registros que ainda não foram geocodificados
        query_busca = """
        SELECT 
            cnpj_basico,
            cnpj_ordem, 
            cnpj_dv,
            logradouro, 
            numero
        FROM estabelecimentos_tratados
        WHERE logradouro IS NOT NULL
        AND numero IS NOT NULL
        """
        
        cursor.execute(query_busca)
        registros = cursor.fetchall()
        
        print(f"Encontrados {len(registros)} registros com logradouro e número...")
        
        # Contadores para estatísticas
        total_processados = 0
        enderecos_validos = 0
        enderecos_invalidos = 0
        
        # Processar cada registro
        for i, registro in enumerate(registros, 1):
            cnpj_basico, cnpj_ordem, cnpj_dv, logradouro, numero = registro
            
            # Construir endereço para geocodificação
            endereco = construir_endereco(logradouro, numero)
            
            if endereco is None:
                enderecos_invalidos += 1
                print(f"Pulando {i}/{len(registros)}: Número inválido - '{numero}'")
                continue
            
            enderecos_validos += 1
            total_processados += 1
            
            print(f"Processando {i}/{len(registros)}: {endereco}")
            
            # Geocodificar endereço
            coordenada_wkt = geocodificar_endereco(endereco)
            
            # Salvar coordenada no banco de dados
            salvar_coordenada(cursor, cnpj_basico, cnpj_ordem, cnpj_dv, coordenada_wkt)
            
            # Commit a cada 10 registros para não perder progresso
            if i % 10 == 0:
                conn.commit()
                print(f"Commit realizado - {i} registros processados")
            
            # Pequena pausa adicional para respeitar a API
            time.sleep(0.1)
        
        # Commit final
        conn.commit()
        
        # Estatísticas
        print(f"\n--- ESTATÍSTICAS ---")
        print(f"Total de registros: {len(registros)}")
        print(f"Endereços válidos: {enderecos_validos}")
        print(f"Endereços inválidos: {enderecos_invalidos}")
        print(f"Total processados: {total_processados}")
        print("Processamento concluído!")
        
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        conn.rollback()
    
    finally:
        conn.close()

def construir_endereco(logradouro, numero):
    """
    Constrói uma string de endereço a partir dos campos individuais
    """
    partes = []
    
    # Apenas logradouro
    if logradouro:
        partes.append(logradouro)
    
    # Apenas número se for um número válido (não S/N)
    if numero and numero.strip().isdigit():
        partes.append(numero)
    
    # Adicionar Garopaba, SC ao final do endereço
    partes.append("Garopaba, SC")
    
    return ", ".join(partes)

def geocodificar_endereco(endereco):
    """
    Geocodifica um endereço e retorna a coordenada WKT
    """
    try:
        location = geocode(endereco)
        
        if location:
            longitude = location.longitude
            latitude = location.latitude
            # Formato WKT para ponto: POINT(longitude latitude)
            return f"POINT({longitude} {latitude})"
        else:
            return "ENDEREÇO NÃO ENCONTRADO"
            
    except Exception as e:
        print(f"Erro ao geocodificar endereço '{endereco}': {e}")
        return f"ERRO: {str(e)}"

def salvar_coordenada(cursor, cnpj_basico, cnpj_ordem, cnpj_dv, coordenada_wkt):
    """
    Salva a coordenada WKT no banco de dados
    """
    try:
        update_query = """
        UPDATE estabelecimentos_tratados 
        SET coordenada_wkt = ? 
        WHERE cnpj_basico = ? AND cnpj_ordem = ? AND cnpj_dv = ?
        """
        
        cursor.execute(update_query, (coordenada_wkt, cnpj_basico, cnpj_ordem, cnpj_dv))
        
    except Exception as e:
        print(f"Erro ao salvar coordenada para CNPJ {cnpj_basico}{cnpj_ordem}{cnpj_dv}: {e}")

if __name__ == "__main__":
    print("Iniciando processo de geocodificação...")
    inicio = time.time()
    
    geocodificar_enderecos()
    
    fim = time.time()
    tempo_total = fim - inicio
    print(f"Tempo total de execução: {tempo_total:.2f} segundos")
