import MetaTrader5 as mt5
from datetime import datetime, timedelta, timezone
import pandas as pd
import asyncio
import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.requests import Request
import threading
import time
import os
import nest_asyncio
import atexit
import gc
import psutil
import numpy as np

nest_asyncio.apply()

# Configuração MT5
mt5_login = 5261751
mt5_password = "Btgmt5@2025"
mt5_server = "BancoBTGPactual-PRD"
symbol = "WINM25"
timeframe = '1Min'

# Encerramento seguro ao fim do script
def encerrar_conexao_mt5():
    mt5.shutdown()
    print("Conexão com MT5 encerrada com sucesso.")
    
# Inicialização única do MT5
if not mt5.initialize(login=mt5_login, password=mt5_password, server=mt5_server):
    raise RuntimeError("Falha ao conectar ao MetaTrader 5")

if not mt5.symbol_select(symbol, True):
    raise RuntimeError(f"Erro ao selecionar o símbolo '{symbol}' no MT5.")

atexit.register(encerrar_conexao_mt5)

def identificar_agressoes(ticks_df):
    """
    Identifica agressões de compra e venda baseado nos flags dos ticks
    Otimizado com operações vetorizadas do pandas
    """
    # Inicializar colunas
    ticks_df['agressao_compra'] = 0.0
    ticks_df['agressao_venda'] = 0.0
    ticks_df['volume_total'] = 0.0
    
    # Converter flags para inteiro para operações bitwise
    flags = ticks_df['flags'].astype(int)
    
    # Máscaras para identificar tipos de flags
    mask_buy = (flags & mt5.TICK_FLAG_BUY) == mt5.TICK_FLAG_BUY
    mask_sell = (flags & mt5.TICK_FLAG_SELL) == mt5.TICK_FLAG_SELL
    mask_both = mask_buy & mask_sell
    mask_only_buy = mask_buy & ~mask_sell
    mask_only_sell = mask_sell & ~mask_buy
    
    # Caso 1: Ambos flags BUY e SELL
    if mask_both.any():
        # Agressão de compra (preço >= ask)
        compra_both = mask_both & (ticks_df['last'] >= ticks_df['ask'])
        ticks_df.loc[compra_both, 'agressao_compra'] = ticks_df.loc[compra_both, 'volume']
        ticks_df.loc[compra_both, 'volume_total'] = ticks_df.loc[compra_both, 'volume']
        
        # Agressão de venda (preço <= bid)
        venda_both = mask_both & (ticks_df['last'] <= ticks_df['bid'])
        ticks_df.loc[venda_both, 'agressao_venda'] = ticks_df.loc[venda_both, 'volume']
        ticks_df.loc[venda_both, 'volume_total'] = ticks_df.loc[venda_both, 'volume']
    
    # Caso 2: Apenas flag BUY
    if mask_only_buy.any():
        ticks_df.loc[mask_only_buy, 'agressao_compra'] = ticks_df.loc[mask_only_buy, 'volume']
        ticks_df.loc[mask_only_buy, 'volume_total'] = ticks_df.loc[mask_only_buy, 'volume']
    
    # Caso 3: Apenas flag SELL
    if mask_only_sell.any():
        ticks_df.loc[mask_only_sell, 'agressao_venda'] = ticks_df.loc[mask_only_sell, 'volume']
        ticks_df.loc[mask_only_sell, 'volume_total'] = ticks_df.loc[mask_only_sell, 'volume']
    
    return ticks_df

def calcular_vwap_diaria_otimizada(candles_df):
    """
    Calcula VWAP diária acumulada usando dados dos candles (muito mais eficiente)
    """
    # Criar colunas auxiliares
    candles_df['date'] = candles_df.index.date
    candles_df['price_volume'] = candles_df['close'] * candles_df['volume']
    
    # Calcular acumulados por dia usando operações vetorizadas
    candles_df['volume_acum_dia'] = candles_df.groupby('date')['volume'].cumsum()
    candles_df['price_volume_acum_dia'] = candles_df.groupby('date')['price_volume'].cumsum()
    
    # Calcular VWAP diária
    candles_df['vwap'] = np.where(
        candles_df['volume_acum_dia'] > 0,
        candles_df['price_volume_acum_dia'] / candles_df['volume_acum_dia'],
        candles_df['close']
    )
    
    # Limpar colunas auxiliares
    candles_df.drop(['date', 'price_volume', 'volume_acum_dia', 'price_volume_acum_dia'], axis=1, inplace=True)
    
    return candles_df

def calcular_features_candle(ticks_grupo):
    """
    Calcula Volume e LS_FlowPulse para um grupo de ticks
    VWAP será calculada separadamente usando dados dos candles
    """
    if len(ticks_grupo) == 0:
        return {
            'volume': 0,
            'ls_flowpulse': 0
        }
    
    # Volume total do minuto
    volume_total = ticks_grupo['volume_total'].sum()
        
    # Agressões de compra e venda
    agressao_compra_total = ticks_grupo['agressao_compra'].sum()
    agressao_venda_total = ticks_grupo['agressao_venda'].sum()
    
    # LS_FlowPulse (ratio das agressões)
    if agressao_venda_total > 0:
        ls_flowpulse = agressao_compra_total / agressao_venda_total
    else:
        ls_flowpulse = agressao_compra_total if agressao_compra_total > 0 else 1.0
    
    return {
        'volume': volume_total,
        'ls_flowpulse': ls_flowpulse
    }

# Histórico inicial
print("📊 Carregando histórico inicial...")
start = datetime.now() - timedelta(days=2)
ticks = mt5.copy_ticks_from(symbol, start, 1000000000, mt5.COPY_TICKS_ALL)
print(f"✅ Carregados {len(ticks)} ticks")

df_ticks = pd.DataFrame(ticks)
df_ticks['time'] = pd.to_datetime(df_ticks['time'], unit='s')
df_ticks.set_index('time', inplace=True)
print(f"📈 DataFrame criado com {len(df_ticks)} registros")
#--------------
# --- Código para remover as linhas ---

# Filtra o DataFrame para incluir apenas as linhas onde 'last' NÃO é igual a 0
df_ticks = df_ticks[df_ticks['last'] != 0]

print(f"📉 DataFrame após remoção de 'last' == 0: {len(df_ticks)} registros")
#--------------
print("🔍 Identificando agressões nos ticks...")
df_ticks = identificar_agressoes(df_ticks)
print("✅ Agressões identificadas com sucesso")

# Manter uma cópia dos ticks para cálculo da VWAP diária
df_ticks_para_vwap = df_ticks.copy()

print("📈 Criando candles com features...")
# Criar candles básicos (OHLC)
candles_df = df_ticks['last'].resample(timeframe).ohlc().dropna()
candles_df.index = candles_df.index.tz_localize('UTC')
print(f"📊 Criados {len(candles_df)} candles")

# Adicionar features para cada candle
print("🔧 Calculando features dos candles...")

# Criar coluna de agrupamento por minuto para os ticks
df_ticks['candle_time'] = df_ticks.index.floor(timeframe).tz_localize('UTC')

# Calcular features básicas (volume e agressões) agrupadas
features_grouped = df_ticks.groupby('candle_time').agg({
    'volume_total': 'sum',
    'agressao_compra': 'sum', 
    'agressao_venda': 'sum'
}).rename(columns={'volume_total': 'volume'})

# Calcular LS_FlowPulse
features_grouped['ls_flowpulse'] = features_grouped.apply(
    lambda row: row['agressao_compra'] / row['agressao_venda'] if row['agressao_venda'] > 0 
    else (row['agressao_compra'] if row['agressao_compra'] > 0 else 1.0), 
    axis=1
)
# 123456
# 123
# 5465
# Fazer merge com candles_df
candles_df = candles_df.join(features_grouped, how='left')

# Preencher valores NaN com zeros (caso não haja ticks para algum minuto)
candles_df[['volume', 'ls_flowpulse']] = candles_df[['volume', 'ls_flowpulse']].fillna(1)

# Calcular VWAP diária de forma otimizada (usando dados dos candles)
print("📊 Calculando VWAP diária otimizada...")
candles_df = calcular_vwap_diaria_otimizada(candles_df)

print(f"✅ Features calculadas para {len(candles_df)} candles")

latest_msc = df_ticks['time_msc'].iloc[-1]

# Armazenar ticks do último minuto para atualização em tempo real
ultimo_minuto = candles_df.index[-1].replace(second=0, microsecond=0)
ticks_ultimo_minuto = df_ticks[df_ticks.index >= ultimo_minuto.tz_localize(None)].copy()

print("🧹 Limpando cache de ticks...")
memory_before = psutil.Process().memory_info().rss / 1024 / 1024

del ticks       # Remove ticks originais
del df_ticks    # Remove DataFrame de ticks original  
del df_ticks_para_vwap  # Não precisamos mais para cálculo em tempo real
gc.collect()    # Força limpeza de memória

memory_after = psutil.Process().memory_info().rss / 1024 / 1024
print(f"💾 Memória economizada: {memory_before - memory_after:.1f} MB")
print(f"📊 Candles carregados: {len(candles_df)}")

# FastAPI setup
app = FastAPI()
base_dir = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(base_dir, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(base_dir, "static")), name="static")

@app.get("/")
def get_chart(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

clients = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)

    # Enviar histórico inicial com features
    data = [
        {
            "time": int(ts.timestamp()),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "vwap": float(row["vwap"]),
            "volume": float(row["volume"]),
            "ls_flowpulse": float(row["ls_flowpulse"])
        }
        for ts, row in candles_df.iterrows()
    ]
    await websocket.send_json({"type": "history", "candles": data})

    try:
        while True:
            await asyncio.sleep(10)
    except:
        if websocket in clients:
            clients.remove(websocket)

def atualizar_ticks(loop):
    global candles_df, latest_msc, ticks_ultimo_minuto

    ticks_anteriores = None

    while True:
        try:
            # Buscar todos os novos ticks desde o último processado
            novos_ticks = mt5.copy_ticks_from(
                symbol,
                datetime.fromtimestamp(latest_msc / 1000, tz=timezone.utc),
                5000,
                mt5.COPY_TICKS_ALL
            )

            if novos_ticks is None or len(novos_ticks) == 0:
                time.sleep(0.001)
                continue

            # Remoção vetorizada de duplicatas usando numpy
            if ticks_anteriores is not None and len(ticks_anteriores) > 0:
                tmsc_n = novos_ticks['time_msc'].astype(str)
                last_n = novos_ticks['last'].astype(str)
                vol_n = novos_ticks['volume'].astype(str)
                id_novos = np.char.add(np.char.add(tmsc_n, "_"), np.char.add(last_n, "_"))
                id_novos = np.char.add(id_novos, vol_n)
            
                tmsc_a = ticks_anteriores['time_msc'].astype(str)
                last_a = ticks_anteriores['last'].astype(str)
                vol_a = ticks_anteriores['volume'].astype(str)
                id_anteriores = np.char.add(np.char.add(tmsc_a, "_"), np.char.add(last_a, "_"))
                id_anteriores = np.char.add(id_anteriores, vol_a)
            
                mask_novos = ~np.isin(id_novos, id_anteriores)
                ticks_filtrados = novos_ticks[mask_novos]
            else:
                ticks_filtrados = novos_ticks

            # Atualizar ticks_anteriores para a próxima iteração
            ticks_anteriores = novos_ticks.copy()

            if len(ticks_filtrados) == 0:
                time.sleep(0.001)
                continue

            print(f"📈 Processando {len(ticks_filtrados)} novos ticks")

            # Processar cada tick novo
            for tick in ticks_filtrados:
                latest_msc = tick['time_msc']
                price = tick['last']
                ts = datetime.fromtimestamp(tick['time'], tz=timezone.utc).replace(second=0, microsecond=0)

                tick_data = {
                    'last': tick['last'],
                    'bid': tick['bid'],
                    'ask': tick['ask'],
                    'volume': tick['volume'],
                    'flags': tick['flags'],
                    'time_msc': tick['time_msc']
                }

                # Identificar agressões do tick atual
                agressao_compra = 0.0
                agressao_venda = 0.0
                volume_total = 0.0
                flags = int(tick['flags'])

                if (flags & mt5.TICK_FLAG_BUY) == mt5.TICK_FLAG_BUY and (flags & mt5.TICK_FLAG_SELL) == mt5.TICK_FLAG_SELL:
                    if tick['last'] >= tick['ask']:
                        agressao_compra = tick['volume']
                        volume_total = tick['volume']
                    elif tick['last'] <= tick['bid']:
                        agressao_venda = tick['volume']
                        volume_total = tick['volume']
                elif (flags & mt5.TICK_FLAG_BUY) == mt5.TICK_FLAG_BUY:
                    agressao_compra = tick['volume']
                    volume_total = tick['volume']
                elif (flags & mt5.TICK_FLAG_SELL) == mt5.TICK_FLAG_SELL:
                    agressao_venda = tick['volume']
                    volume_total = tick['volume']

                tick_data['agressao_compra'] = agressao_compra
                tick_data['agressao_venda'] = agressao_venda
                tick_data['volume_total'] = volume_total

                # Verificar se é novo candle ou atualização do candle atual
                if ts in candles_df.index:
                    candles_df.loc[ts, ["high", "low", "close"]] = [
                        max(candles_df.loc[ts, "high"], price),
                        min(candles_df.loc[ts, "low"], price),
                        price
                    ]
                    tick_time = datetime.fromtimestamp(tick['time'])
                    tick_series = pd.Series(tick_data, name=tick_time)
                    ticks_ultimo_minuto = pd.concat([ticks_ultimo_minuto, tick_series.to_frame().T])
                else:
                    nova_linha = pd.Series({
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': 0,
                        'ls_flowpulse': 0,
                        'vwap': price
                    }, name=ts)
                    candles_df = pd.concat([candles_df, nova_linha.to_frame().T])
                    candles_df = candles_df.sort_index()
                    tick_time = datetime.fromtimestamp(tick['time'])
                    tick_series = pd.Series(tick_data, name=tick_time)
                    ticks_ultimo_minuto = tick_series.to_frame().T
                    print(f"🆕 Novo candle criado para {ts}")

            # Após processar todos os ticks, recalcular features uma única vez
            ultimo_ts = datetime.fromtimestamp(ticks_filtrados[-1]['time'], tz=timezone.utc).replace(second=0, microsecond=0)

            features = calcular_features_candle(ticks_ultimo_minuto)
            candles_df.loc[ultimo_ts, 'volume'] = features['volume']
            candles_df.loc[ultimo_ts, 'ls_flowpulse'] = features['ls_flowpulse']

            candles_temp = candles_df.copy()
            candles_temp = calcular_vwap_diaria_otimizada(candles_temp)
            candles_df['vwap'] = candles_temp['vwap']

            row = candles_df.loc[ultimo_ts]
            candle = {
                "time": int(ultimo_ts.timestamp()),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "vwap": float(row["vwap"]),
                "volume": float(row["volume"]),
                "ls_flowpulse": float(row["ls_flowpulse"])
            }

            for ws in clients[:]:
                try:
                    asyncio.run_coroutine_threadsafe(
                        ws.send_json({"type": "update", "candle": candle}),
                        loop
                    )
                except Exception as e:
                    print(f"Erro ao enviar candle para WebSocket: {e}")
                    try:
                        clients.remove(ws)
                    except ValueError:
                        pass

        except Exception as e:
            print(f"❌ Erro no loop de atualização de ticks: {e}")

        time.sleep(0.001)

# Início do loop de atualização em thread separada
main_loop = asyncio.get_event_loop()
threading.Thread(target=atualizar_ticks, args=(main_loop,), daemon=True).start()

if __name__ == "__main__":
    try:
        uvicorn.run(app, host="0.0.0.0", port=8001)
    except OSError as e:
        print(f"Erro ao iniciar servidor: {e}")
