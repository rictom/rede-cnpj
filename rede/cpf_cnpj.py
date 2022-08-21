# -*- coding: utf-8 -*-

import re

def validar_cpf(cpf):
    """
    Valida CPFs, retornando apenas a string de números válida.
    se tiver menos de 11 algarismos, preenche
    """
    cpf = ''.join(re.findall('\d', str(cpf)))
    
    if (not cpf):
        return ''
    
    if (len(cpf) > 11):
        if cpf[:len(cpf)-11]=='0'.zfill(len(cpf)-11):
            cpf = cpf[len(cpf)-11:]
        else:
            return ''
    if (len(cpf) < 11):
        cpf = cpf.zfill(11)

    numbers = [int(digit) for digit in cpf if digit.isdigit()]

    # Validação do primeiro dígito verificador:
    sum_of_products = sum(a*b for a, b in zip(numbers[0:9], range(10, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[9] != expected_digit:
        return ''

    # Validação do segundo dígito verificador:
    sum_of_products = sum(a*b for a, b in zip(numbers[0:10], range(11, 1, -1)))
    expected_digit = (sum_of_products * 10 % 11) % 10
    if numbers[10] != expected_digit:
        return ''

    return cpf

def validar_cnpj(cnpj):
    """
    Valida CNPJs, retornando apenas a string de números válida.
    """

    cnpj = ''.join(re.findall('\d', str(cnpj)))
    
    if (not cnpj):
        return ''
    
    if (len(cnpj) > 14):
        if cnpj[:len(cnpj)-14]=='0'.zfill(len(cnpj)-14):
            cnpj = cnpj[len(cnpj)-14:]
        else:
            return ''
    cnpj = cnpj.zfill(14)
    
    cnpj = ''.join(re.findall('\d', str(cnpj)))

    
    # Pega apenas os 12 primeiros dígitos do CNPJ e gera os 2 dígitos que faltam
    inteiros = list(map(int, cnpj))
    novo = inteiros[:12]
    
    prod = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    while len(novo) < 14:
        r = sum([x*y for (x, y) in zip(novo, prod)]) % 11
        if r > 1:
            f = 11 - r
        else:
            f = 0
        novo.append(f)
        prod.insert(0, 6)
    
    # Se o número gerado coincidir com o número original, é válido
    if novo == inteiros:
        return cnpj
    return ''
