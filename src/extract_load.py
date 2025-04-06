import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os
import logging
from typing import List, Optional
from datetime import datetime

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carrega variáveis do .env
load_dotenv()

class FinancialDataETL:
    def __init__(self):
        self._validate_env_vars()
        self.engine = self._create_db_engine()
        self.tickers = [
            'USDBRL=X', 'BTC-USD', 'ETH-USD', 
            'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 
            'B3SA3.SA', 'BBAS3.SA', 'BBSE3.SA', 
            'ABEV3.SA'
        ]
        
    def _validate_env_vars(self) -> None:
        """Valida se todas as variáveis de ambiente necessárias estão definidas"""
        required_vars = [
            'DB_HOST_PROD', 'DB_PORT_PROD', 'DB_NAME_PROD',
            'DB_USER_PROD', 'DB_PASS_PROD', 'DB_SCHEMA_PROD'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(
                f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}"
            )
    
    def _create_db_engine(self):
        """Cria e retorna uma conexão com o banco de dados"""
        db_url = (
            f"postgresql://{os.getenv('DB_USER_PROD')}:{os.getenv('DB_PASS_PROD')}@"
            f"{os.getenv('DB_HOST_PROD')}:{os.getenv('DB_PORT_PROD')}/"
            f"{os.getenv('DB_NAME_PROD')}"
        )
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            logger.info("Engine de banco de dados criado com sucesso")
            return engine
        except exc.SQLAlchemyError as e:
            logger.error(f"Erro ao criar engine do banco de dados: {e}")
            raise
    
    @staticmethod
    def fetch_ticker_data(
        ticker: str, 
        period: str = '5d', 
        interval: str = '1d',
        retries: int = 3
    ) -> Optional[pd.DataFrame]:
        """
        Busca dados históricos de um ticker específico no Yahoo Finance
        
        Args:
            ticker: Símbolo do ativo
            period: Período de tempo (padrão: 5 dias)
            interval: Intervalo entre dados (padrão: 1 dia)
            retries: Número de tentativas em caso de falha
            
        Returns:
            DataFrame com os dados ou None em caso de falha
        """
        for attempt in range(retries):
            try:
                tick = yf.Ticker(ticker)
                data = tick.history(period=period, interval=interval)[['Close']]
                data['ticker'] = ticker
                data['data_coleta'] = datetime.now()
                logger.info(f"Dados para {ticker} obtidos com sucesso")
                return data
            except Exception as e:
                logger.warning(
                    f"Tentativa {attempt + 1} para {ticker} falhou: {str(e)}"
                )
                if attempt == retries - 1:
                    logger.error(f"Falha ao obter dados para {ticker} após {retries} tentativas")
                    return None
    
    def fetch_all_tickers_data(self) -> pd.DataFrame:
        """
        Busca dados para todos os tickers na lista
        
        Returns:
            DataFrame concatenado com todos os dados
        """
        all_data = []
        
        for ticker in self.tickers:
            data = self.fetch_ticker_data(ticker)
            if data is not None:
                all_data.append(data)
        
        if not all_data:
            raise ValueError("Nenhum dado foi obtido para os tickers")
            
        return pd.concat(all_data)
    
    def save_to_postgres(
        self, 
        df: pd.DataFrame, 
        table_name: str = 'tickers',
        schema: str = None,
        if_exists: str = 'replace'
    ) -> None:
        """
        Salva os dados no PostgreSQL
        
        Args:
            df: DataFrame com os dados
            table_name: Nome da tabela de destino
            schema: Schema do banco de dados
            if_exists: Comportamento se tabela existir ('replace', 'append', 'fail')
        """
        if schema is None:
            schema = os.getenv('DB_SCHEMA_PROD', 'public')
        
        try:
            with self.engine.begin() as connection:
                df.to_sql(
                    name=table_name,
                    con=connection,
                    if_exists=if_exists,
                    index=True,
                    index_label='date',
                    schema=schema,
                    method='multi',
                    chunksize=1000
                )
            logger.info(
                f"Dados salvos com sucesso na tabela {schema}.{table_name}"
            )
        except exc.SQLAlchemyError as e:
            logger.error(f"Erro ao salvar dados no PostgreSQL: {e}")
            raise
    
    def run_etl(self) -> None:
        """Executa o processo completo de ETL"""
        try:
            logger.info("Iniciando processo ETL")
            data = self.fetch_all_tickers_data()
            self.save_to_postgres(data)
            logger.info("Processo ETL concluído com sucesso")
        except Exception as e:
            logger.error(f"Falha no processo ETL: {e}")
            raise


if __name__ == "__main__":
    try:
        etl = FinancialDataETL()
        etl.run_etl()
    except Exception as e:
        logger.critical(f"Erro crítico na execução: {e}")
        exit(1)