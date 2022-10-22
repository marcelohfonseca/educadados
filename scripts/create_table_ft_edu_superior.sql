CREATE TABLE IF NOT EXISTS `EDUCADADOS`.`FT_EDU_SUPERIOR` (
    `NR_ANO` INT NOT NULL,
    `CD_IES` INT NOT NULL,
    `CD_MUNICIPIO` INT NOT NULL,
    `CD_CURSO` VARCHAR(80) NOT NULL,
    `QT_CURSO` INT NULL,
    `QT_VG_TOTAL` INT NULL,
    `QT_VG_TOTAL_DIURNO` INT NULL,
    `QT_VG_TOTAL_NOTURNO` INT NULL,
    `QT_VG_TOTAL_EAD` INT NULL,
    `QT_VG_NOVA` INT NULL,
    `QT_VG_REMANESC` INT NULL,
    `QT_VG_PROG_ESPECIAL` INT NULL,
    `QT_INSCRITO_TOTAL` INT NULL,
    `QT_INSCRITO_TOTAL_DIURNO` INT NULL,
    `QT_INSCRITO_TOTAL_NOTURNO` INT NULL,
    `QT_INSCRITO_TOTAL_EAD` INT NULL,
    `QT_INSC_VG_NOVA` INT NULL,
    `QT_INSC_VG_REMANESC` INT NULL,
    `QT_INSC_VG_PROG_ESPECIAL` INT NULL,
    `QT_ING` INT NULL,
    `QT_ING_FEM` INT NULL,
    `QT_ING_MASC` INT NULL,
    `QT_ING_DIURNO` INT NULL,
    `QT_ING_NOTURNO` INT NULL,
    `QT_ING_VG_NOVA` INT NULL,
    `QT_ING_VESTIBULAR` INT NULL,
    `QT_ING_ENEM` INT NULL,
    `QT_ING_AVALIACAO_SERIADA` INT NULL,
    `QT_ING_SELECAO_SIMPLIFICA` INT NULL,
    `QT_ING_EGR` INT NULL,
    `QT_ING_VG_REMANESC` INT NULL,
    `QT_ING_VG_PROG_ESPECIAL` INT NULL,
    `QT_ING_OUTRA_FORMA` INT NULL,
    `QT_ING_0_17` INT NULL,
    `QT_ING_18_24` INT NULL,
    `QT_ING_25_29` INT NULL,
    `QT_ING_30_34` INT NULL,
    `QT_ING_35_39` INT NULL,
    `QT_ING_40_49` INT NULL,
    `QT_ING_50_59` INT NULL,
    `QT_ING_60_MAIS` INT NULL,
    `QT_ING_BRANCA` INT NULL,
    `QT_ING_PRETA` INT NULL,
    `QT_ING_PARDA` INT NULL,
    `QT_ING_AMARELA` INT NULL,
    `QT_ING_INDIGENA` INT NULL,
    `QT_ING_CORND` INT NULL,
    `QT_ING_NACBRAS` INT NULL,
    `QT_ING_NACESTRANG` INT NULL,
    `QT_ING_FINANC` INT NULL,
    `QT_ING_FINANC_REEMB` INT NULL,
    `QT_ING_FIES` INT NULL,
    `QT_ING_RPFIES` INT NULL,
    `QT_ING_FINANC_REEMB_OUTROS` INT NULL,
    `QT_ING_FINANC_NREEMB` INT NULL,
    `QT_ING_PROUNII` INT NULL,
    `QT_ING_PROUNIP` INT NULL,
    `QT_ING_NRPFIES` INT NULL,
    `QT_ING_FINANC_NREEMB_OUTROS` INT NULL,
    `QT_ING_DEFICIENTE` INT NULL,
    `QT_ING_RESERVA_VAGA` INT NULL,
    `QT_ING_RVREDEPUBLICA` INT NULL,
    `QT_ING_RVETNICO` INT NULL,
    `QT_ING_RVPDEF` INT NULL,
    `QT_ING_RVSOCIAL_RF` INT NULL,
    `QT_ING_RVOUTROS` INT NULL,
    `QT_ING_PROCESCPUBLICA` INT NULL,
    `QT_ING_PROCESCPRIVADA` INT NULL,
    `QT_ING_PROCNAOINFORMADA` INT NULL,
    `QT_ING_PARFOR` INT NULL,
    `QT_ING_APOIO_SOCIAL` INT NULL,
    `QT_ING_ATIV_EXTRACURRICULAR` INT NULL,
    `QT_ING_MOB_ACADEMICA` INT NULL,
    `QT_MAT` INT NULL,
    `QT_MAT_FEM` INT NULL,
    `QT_MAT_MASC` INT NULL,
    `QT_MAT_DIURNO` INT NULL,
    `QT_MAT_NOTURNO` INT NULL,
    `QT_MAT_0_17` INT NULL,
    `QT_MAT_18_24` INT NULL,
    `QT_MAT_25_29` INT NULL,
    `QT_MAT_30_34` INT NULL,
    `QT_MAT_35_39` INT NULL,
    `QT_MAT_40_49` INT NULL,
    `QT_MAT_50_59` INT NULL,
    `QT_MAT_60_MAIS` INT NULL,
    `QT_MAT_BRANCA` INT NULL,
    `QT_MAT_PRETA` INT NULL,
    `QT_MAT_PARDA` INT NULL,
    `QT_MAT_AMARELA` INT NULL,
    `QT_MAT_INDIGENA` INT NULL,
    `QT_MAT_CORND` INT NULL,
    `QT_MAT_NACBRAS` INT NULL,
    `QT_MAT_NACESTRANG` INT NULL,
    `QT_MAT_DEFICIENTE` INT NULL,
    `QT_MAT_FINANC` INT NULL,
    `QT_MAT_FINANC_REEMB` INT NULL,
    `QT_MAT_FIES` INT NULL,
    `QT_MAT_RPFIES` INT NULL,
    `QT_MAT_FINANC_REEMB_OUTROS` INT NULL,
    `QT_MAT_FINANC_NREEMB` INT NULL,
    `QT_MAT_PROUNII` INT NULL,
    `QT_MAT_PROUNIP` INT NULL,
    `QT_MAT_NRPFIES` INT NULL,
    `QT_MAT_FINANC_NREEMB_OUTROS` INT NULL,
    `QT_MAT_RESERVA_VAGA` INT NULL,
    `QT_MAT_RVREDEPUBLICA` INT NULL,
    `QT_MAT_RVETNICO` INT NULL,
    `QT_MAT_RVPDEF` INT NULL,
    `QT_MAT_RVSOCIAL_RF` INT NULL,
    `QT_MAT_RVOUTROS` INT NULL,
    `QT_MAT_PROCESCPUBLICA` INT NULL,
    `QT_MAT_PROCESCPRIVADA` INT NULL,
    `QT_MAT_PROCNAOINFORMADA` INT NULL,
    `QT_MAT_PARFOR` INT NULL,
    `QT_MAT_APOIO_SOCIAL` INT NULL,
    `QT_MAT_ATIV_EXTRACURRICULAR` INT NULL,
    `QT_MAT_MOB_ACADEMICA` INT NULL,
    `QT_CONC` INT NULL,
    `QT_CONC_FEM` INT NULL,
    `QT_CONC_MASC` INT NULL,
    `QT_CONC_DIURNO` INT NULL,
    `QT_CONC_NOTURNO` INT NULL,
    `QT_CONC_0_17` INT NULL,
    `QT_CONC_18_24` INT NULL,
    `QT_CONC_25_29` INT NULL,
    `QT_CONC_30_34` INT NULL,
    `QT_CONC_35_39` INT NULL,
    `QT_CONC_40_49` INT NULL,
    `QT_CONC_50_59` INT NULL,
    `QT_CONC_60_MAIS` INT NULL,
    `QT_CONC_BRANCA` INT NULL,
    `QT_CONC_PRETA` INT NULL,
    `QT_CONC_PARDA` INT NULL,
    `QT_CONC_AMARELA` INT NULL,
    `QT_CONC_INDIGENA` INT NULL,
    `QT_CONC_CORND` INT NULL,
    `QT_CONC_NACBRAS` INT NULL,
    `QT_CONC_NACESTRANG` INT NULL,
    `QT_CONC_DEFICIENTE` INT NULL,
    `QT_CONC_FINANC` INT NULL,
    `QT_CONC_FINANC_REEMB` INT NULL,
    `QT_CONC_FIES` INT NULL,
    `QT_CONC_RPFIES` INT NULL,
    `QT_CONC_FINANC_REEMB_OUTROS` INT NULL,
    `QT_CONC_FINANC_NREEMB` INT NULL,
    `QT_CONC_PROUNII` INT NULL,
    `QT_CONC_PROUNIP` INT NULL,
    `QT_CONC_NRPFIES` INT NULL,
    `QT_CONC_FINANC_NREEMB_OUTROS` INT NULL,
    `QT_CONC_RESERVA_VAGA` INT NULL,
    `QT_CONC_RVREDEPUBLICA` INT NULL,
    `QT_CONC_RVETNICO` INT NULL,
    `QT_CONC_RVPDEF` INT NULL,
    `QT_CONC_RVSOCIAL_RF` INT NULL,
    `QT_CONC_RVOUTROS` INT NULL,
    `QT_CONC_PROCESCPUBLICA` INT NULL,
    `QT_CONC_PROCESCPRIVADA` INT NULL,
    `QT_CONC_PROCNAOINFORMADA` INT NULL,
    `QT_CONC_PARFOR` INT NULL,
    `QT_CONC_APOIO_SOCIAL` INT NULL,
    `QT_CONC_ATIV_EXTRACURRICULAR` INT NULL,
    `QT_CONC_MOB_ACADEMICA` INT NULL,
    `QT_SIT_TRANCADA` INT NULL,
    `QT_SIT_DESVINCULADO` INT NULL,
    `QT_SIT_TRANSFERIDO` INT NULL,
    `QT_SIT_FALECIDO` INT NULL,
    `QT_PARFOR` INT NULL,
    `QT_APOIO_SOCIAL` INT NULL,
    `QT_ATIV_EXTRACURRICULAR` INT NULL,
    `QT_MOB_ACADEMICA` INT NULL,
    `QT_ALUNO_DEFICIENTE` INT NULL,
    PRIMARY KEY (`NR_ANO`, `CD_IES`, `CD_CURSO`, `CD_MUNICIPIO`),
);