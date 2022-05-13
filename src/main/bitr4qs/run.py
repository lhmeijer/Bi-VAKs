import sys
import os

from src.main.bitr4qs.webservice.app import create_app
from src.main.bitr4qs.configuration import get_default_configuration

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')))

defaults = get_default_configuration()
args = {**defaults}
application = create_app(args)

if __name__ == "__main__":
    application.run(host=args['host'], port=args['port'])