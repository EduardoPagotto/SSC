'''
Created on 20220401
Update on 20221123
@author: Eduardo Pagotto
'''

from os.path import basename
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

from typing import Dict, List, Tuple

class SenderSMTP(object):
    """[WRapper Conexao servidor de email]

    Args:
        object ([type]): [description]
    """

    def __init__(self, cfg : dict):

        self.log = logging.getLogger('SenderSMTP')
        self.host : str = cfg['host']
        self.port : int = cfg['port']
        self.auth : Dict = cfg['auth'] if 'auth' in cfg else None

        self.vFrom = cfg['from']
        self.vTo = cfg['to']

    def __autenticacao(self) -> smtplib.SMTP:
        """Cria conexao ao servico de email

        Returns:

            smtplib.SMTP: smtp conectado
        """
        smtp_conn = smtplib.SMTP(self.host, self.port )
        if self.auth is not None:
            self.log.info('Autenticando servidor SMTP')
            smtp_conn.starttls()
            smtp_conn.login(self.auth['user'], self.auth['pass'])

        return smtp_conn

    def send_mail(self, sender_data : dict, is_html : bool, files=None)->Tuple[bool, str]:
        
        try:

            comm = self.__autenticacao()

            msg = MIMEMultipart()
            msg['From'] = self.vFrom #sender_data['from'] 
            msg['To'] = self.vTo # sender_data['to'] 
            msg['Subject'] = sender_data['subject'] #subject
            msg['Date'] = formatdate(localtime=True)
            msg.attach(MIMEText(sender_data['body'], "html") if is_html else MIMEText(sender_data['body'], "plain"))

            for f in files or []:
                with open(f, "rb") as fil:
                    part = MIMEApplication(fil.read(),Name=basename(f))

                # After the file is closed
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
                msg.attach(part)

            comm.sendmail(self.vFrom, self.vTo.split(';'), msg.as_string())

        except Exception as ex:
            #self.log.error('Envio de email' +  sender_data['subject'] + 'falhou:' + str(ex))
            return False, 'Envio de email' +  sender_data['subject'] + 'falhou:' + str(ex)

        #self.log.debug()
        return True, f'Email {sender_data["subject"]} enviado: {self.vTo}'
