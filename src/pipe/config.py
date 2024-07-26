s3_bucket = 's3://datalake-data-sus/'

filter_columns = [
    'ANO_CMPT',
    'MES_CMPT',
    'CEP',
    'SEXO',
    'RACA_COR',
    'INSTRU',
    'IDADE',
    'DIAG_PRINC',
    'DIAS_PERM',
    'US_TOT'
]

dicionario_sexo   = {'1':'Masculino', '3':'Feminino'}

dicionario_raca   = {'99':'ND', '01':'Branca', '02':'Negra', '03':'Parda', '04':'Amarela', '05':'Indigena'}

dicionario_instru = {'0':'ND', '1':'Analfabeto', '2':'1_Grau', '3':'2_Grau', '4':'Superior'}

dicionario_diag_class = {"CARDIOVASCULAR": ["I00", "I01", "I02", "I03", "I04", "I05", "I06", "I07", "I08", "I09", "I10", "I11", "I12", "I13", "I14", "I15", "I16", "I17", "I18", "I19", "I20", "I21", "I22", "I23", "I24", "I25", "I26", "I27", "I28", "I29", "I30", "I31", "I32", "I33", "I34", "I35", "I36", "I37", "I38", "I39", "I40", "I41", "I42", "I43", "I44", "I45", "I46", "I47", "I48", "I49", "I50", "I51", "I52", "I53", "I54", "I55", "I56", "I57", "I58", "I59", "I60", "I61", "I62", "I63", "I64", "I65", "I66", "I67", "I68", "I69", "I70", "I71", "I72", "I73", "I74", "I75", "I76", "I77", "I78", "I79", "I80", "I81", "I82", "I83", "I84", "I85", "I86", "I87", "I88", "I89", "I90", "I91", "I92", "I93", "I94", "I95", "I96", "I97", "I98", "I99"], "RESPIRATORIO": ["J30", "J31", "J32", "J33", "J34", "J35", "J36", "J37", "J38", "J39", "J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47", "J48", "J49", "J50", "J51", "J52", "J53", "J54", "J55", "J56", "J57", "J58", "J59", "J60", "J61", "J62", "J63", "J64", "J65", "J66", "J67", "J68", "J69", "J70", "J71", "J72", "J73", "J74", "J75", "J76", "J77", "J78", "J79", "J80", "J81", "J82", "J83", "J84", "J85", "J86", "J87", "J88", "J89", "J90", "J91", "J92", "J93", "J94", "J95", "J96", "J97", "J98"], "DIABTES": ["E10", "E11", "E12", "E13", "E14"], "OBESIDADE": ["E65", "E66", "E67", "E68"], "NEPLASIA": ["C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26", "C27", "C28", "C29", "C30", "C31", "C32", "C33", "C34", "C35", "C36", "C37", "C38", "C39", "C40", "C41", "C42", "C43", "C44", "C45", "C46", "C47", "C48", "C49", "C50", "C51", "C52", "C53", "C54", "C55", "C56", "C57", "C58", "C59", "C60", "C61", "C62", "C63", "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C77", "C78", "C79", "C80", "C81", "C82", "C83", "C84", "C85", "C86", "C87", "C88", "C89", "C90", "C91", "C92", "C93", "C94", "C95", "C96", "C97"], "FIGADO": ["K70", "K71", "K72", "K73", "K74"], "RENAL": ["N03", "N04", "N05", "N13", "N14", "N15", "N16", "N11", "N18"]}
