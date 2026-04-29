def get_flexible_input(prompt: str, default_yes: bool = False) -> bool:
    """
    Interpreta input flexible del usuario
    SI: s, si, sí, simon, simón, yes, y, "" (enter cuando default_yes=True)
    NO: n, no, nel, q, quit, exit
    """
    response = input(prompt).strip().lower()
    
    if response == "":
        return default_yes
    
    yes_responses = {'s', 'si', 'sí', 'simon', 'simón', 'yes', 'y'}
    no_responses = {'n', 'no', 'nel', 'q', 'quit', 'exit'}
    
    if response in yes_responses:
        return True
    if response in no_responses:
        return False
    
    print("❌ Respuesta no reconocida. Responde 's' (sí) o 'n' (no).")
    return get_flexible_input(prompt, default_yes)