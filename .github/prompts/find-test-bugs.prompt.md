---
description: Confronta i test di unità con il codice operativo ed evidenzia se i test sono scritti per far passare bug invece che evitarli.
mode: agent
---

# 🧪 Confronto tra Unit Test e Codice Operativo

Analizza il codice operativo e i relativi test di unità nella workspace corrente (o nei file forniti).  
Obiettivo: verificare se i test **coprono realmente i comportamenti attesi** o se sono stati costruiti per mascherare bug ed errori logici.

## Requisiti di output

1. **Sommario esecutivo**  
   - Panoramica generale della qualità e affidabilità dei test.

2. **Mappatura codice ↔ test**  
   - Per ogni funzione/metodo principale, elenca quali test lo coprono.  
   - Specifica se i test verificano **casi nominali** e/o **casi limite/errore**.  
   - Evidenzia parti di codice non testate.

3. **Individuazione di test sospetti**  
   - Test che verificano solo un percorso positivo ignorando errori.  
   - Test che replicano un bug senza realmente fallire quando il bug è presente.  
   - Test che contengono assert troppo deboli (es. solo `not nil`, ma non controllano logica).  
   - Test con **hardcoded values** che coincidono con un bug.  
   - Test che forzano lo stato del sistema in modo innaturale per far passare il codice.

4. **Evidenza concreta**  
   - File e righe sia del codice che dei test correlati.  
   - Snippet ≤ 25 righe che mostrano la relazione sospetta.  
   - Spiegazione tecnica del potenziale bug che i test non coprono o mascherano.

5. **Raccomandazioni**  
   - Suggerisci come rafforzare i test (assert più precisi, test negativi, edge cases).  
   - Proponi se scrivere nuovi test o correggere gli esistenti.  
   - Eventuali framework o tool (es. mutation testing) per validare la robustezza dei test.

6. **Output atteso**  
   - Markdown strutturato:  
     - ### Sommario  
     - ### Mapping codice ↔ test  
     - ### Test sospetti  
     - ### Raccomandazioni  

## Vincoli
- Non modificare direttamente i file.  
- Limita l’analisi a codice operativo + unit test.  
- Se non trovi test sospetti, spiega perché i test sono robusti.  

## Variabili disponibili
- File corrente: `${file}`  
- Selezione corrente: `${selection}`  
- Workspace: `${workspaceFolder}`

Crea un file nella directory "context" con il nome "find-test-bugs-output-{timestamp}.md" e scrivi lì il risultato.