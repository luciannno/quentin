import Quote

symbols = ["^STOXX50E",
    "ORA.PA",
"ASML.AS",
"AIR.PA",
"SU.PA",
"ABI.BR",
"IBE.MC",
"BN.PA",
"ITX.MC",
"DTE.DE",
"DPW.DE",
"EI.PA",
"ENEL.MI",
"SAN.PA",
"ALV.DE",
"ENI.MI",
"BAYN.DE",
"OR.PA",
"ENGI.PA",
"INGA.AS",
"FRE.DE",
"CA.PA",
"MC.PA",
"SAF.PA",
"AI.PA",
"BNP.PA",
"G.MI",
"BMW.DE",
"BBVA.MC",
"PHIA.AS",
"DBK.DE"]

for s in symbols:
    try:
        q = Quote.Lookup(s)
        print q.__dict__
    except:
        print "Error:", s



