# Importation refactorisée - Point d'entrée principal
"""
Script d'importation refactorisé pour WeatherHub.
Remplace l'ancien Importation.py monolithique.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import main

if __name__ == "__main__":
    main()
