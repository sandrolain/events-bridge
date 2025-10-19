---
description: Identifica opportunità di refactoring e de-duplicazione del codice senza modificare i file.
mode: agent
---

# 🔧 Refactoring & De-duplicazione del codice

Analizza il codice nella workspace corrente (o nel file fornito) e individua duplicazioni, pattern ripetuti, funzioni troppo complesse o aree che possono essere rifattorizzate.  
**Non modificare direttamente i file**, limita l’output a proposte strutturate.

## Requisiti di output

1. **Sommario esecutivo**  
   - Breve panoramica (1-2 frasi) delle principali opportunità di refactoring o semplificazione.

2. **Punti di refactoring**  
   Per ciascun punto trovato, fornisci:  
   - File e range di righe (o funzione/class)  
   - Tipo di problema (duplicazione / funzione troppo lunga / codice non modulare / variabile mal nominata / ecc.)  
   - Evidenza (snippet ≤ 25 righe)  
   - Impatto (leggibilità / manutenibilità / performance / riuso)  
   - Complessità prevista della modifica (Bassa / Media / Alta)

3. **Proposta di miglioramento**  
   - Descrivi passo-passo come rifattorizzare  
   - Mostra eventuale **diff minimale** o snippet di codice alternativo  
   - Spiega benefici e rischi  
   - Indica se il refactoring può essere fatto incrementale (in più PR) o richiede un big-bang

4. **De-duplicazione del codice**  
   - Evidenzia funzioni o blocchi ripetuti in più file  
   - Proponi un estratto comune (funzione, metodo, libreria interna, package, componente UI…)  
   - Mostra esempio di come centralizzare il codice duplicato  
   - Suggerisci naming chiaro e posizionamento

5. **Linee guida generali**  
   - Suggerisci standard di clean code applicabili al progetto  
   - Eventuali tool automatici consigliati (linters, formatters, code smell detectors) con comandi concreti da eseguire

## Vincoli
- Non modificare i file della workspace.  
- Non limitarti a elencare, ma spiega come e perché rifattorizzare.  
- Usa esempi concreti e diff minimali.  
- Se non ci sono duplicazioni significative, spiega perché.  

## Variabili disponibili
- File corrente: `${file}`  
- Selezione corrente: `${selection}`  
- Workspace: `${workspaceFolder}`  

Crea un file nella directory "context" con il nome "need-to-refactor-output-{timestamp}.md" e scrivi lì il risultato.