import pandas as pd
from pulp import *
import sys
import psycopg2
from os import remove

constraint_data = pd.read_csv(sys.argv[1], delimiter=';')

conn = psycopg2.connect(
    host="ffac.cvn7inqnh68j.us-east-2.rds.amazonaws.com",
    database="postgres",
    user="python",
    password="917aRV$qq2Tf")
cur = conn.cursor()
cur.execute("delete from generic.ex_solucion;")
conn.commit()

es_entero=sys.argv[3]=='discreto'
maxSeconds=sys.argv[4]
Tolerance=sys.argv[5]

#cons_data = pdf[pdf.columns[3:]]
#constraint_data = {cons_data[k][0]: cons_data[k][1:].values
#                for k in cons_data}
#for k in ['MIN_VOL', 'MAX_VOL']:
#    constraint_data[k] = [float(v) for v in constraint_data[k]]

stand_data = pd.read_csv(sys.argv[2])

nSrows = len(stand_data); nScols = len(stand_data.iloc[0])
print(f'Stand_data: {nSrows} rows x{nScols} columns')
print('problema discreto?', es_entero)
print('MaxSeconds:', maxSeconds)
print('Tolerance:', Tolerance)

#shnv = list(zip(stand_data['Stand_id'], stand_data['HarvestYear'],
#            stand_data['NPV_tot'], stand_data['Tot_Vol']))

OPERATION_YEARS = [col for col in stand_data.columns 
						if col.startswith('operation')]
VOL_COLUMNS = [col for col in stand_data.columns 
						if col.startswith('volumen')]
OpYearsIndices = [ix for ix,col in enumerate(stand_data.columns)
						if col in OPERATION_YEARS]
VolIndices = [ix for ix,col in enumerate(stand_data.columns)
						if col in VOL_COLUMNS]						
nRotations = len(OPERATION_YEARS)  # GENERICO

sn = list(zip(stand_data['Stand_id'],    # Stand_id=unit_id
            stand_data['NPV_tot']))		     # NPV_tot = npv_current

cost={(s[0],[int(s[ix]) for ix in OpYearsIndices]): float(s[1]) for s in shnv}						  # (Stand100,(2030,2048,2066)): 3923
vols={(s[0],[int(s[ix]) for ix in OpYearsIndices]): [int(s[ix]) for ix in VolIndices] for s in shnv}  # (Stand100,(2030,2048,2066)): [99,99,99]
   
possible_combinations = cost.keys()
standnames = set(stand_data['Stand_id'])
print('nStands:', len(standnames))
##########################################
#constraint_data = get_constraints()
#print(constraint_data)
z = LpVariable('z')
if es_entero:  # soluciones enteras
    x = LpVariable.dicts('x', possible_combinations, lowBound=0,upBound=1, cat='Integer')
    print('Variable de Cosecha Entera')
else:
    x = LpVariable.dicts('x', possible_combinations, lowBound=0,upBound=1)
    print('Variable de Cosecha CONTINUA')

YEARS = list(set([k[1] for k in cost.keys()]))    # (2030,2048,2066),(2030,2048,2067),...

yearly_volume = LpVariable.dicts('yearly_volume', YEARS, lowBound=0)  # harvested volume per year
StandHarvest = LpVariable.dicts('SC', standnames, lowBound=0,upBound=1) #,cat=pulp.LpInteger)
StandVolumes = LpVariable.dicts('SV', standnames, lowBound=0) #,cat=pulp.LpInteger)
   
prob = LpProblem('NPV', LpMaximize)
prob += z, "objective"
prob += z== lpSum([cost[pc] * x[pc] for pc in possible_combinations]), "objective_definition"
#prob += lpSum(
min_vols = {}; max_vols={}; nyrc=0

for year in YEARS:      # define AND constrain yearly_volume
    prob += lpSum([vols[pc]*x[pc] for pc in possible_combinations if pc[1] == year]) \
                           == yearly_volume[year]  
    # find year in ranges
    for yrs, minvol, maxvol in list(zip(constraint_data['Year Range'], constraint_data['MIN_VOL'], constraint_data['MAX_VOL'])):
        years = [int(yr) for yr in yrs.split('-')]
        yearly_range = range(years[0], years[1]+1)
        if year in yearly_range:
            #print('%d is in (%d,%d)' %(year,years[0],years[1]) )
            prob += minvol<=yearly_volume[year]
            prob += yearly_volume[year]<=maxvol
            min_vols[year] = minvol
            max_vols[year] = maxvol
            nyrc+=1
           
print('nYearCons:', nyrc, ', nYears:', len(YEARS))
for stand in standnames:
    prob+=StandHarvest[stand] <= 1.0
    prob+=StandHarvest[stand] == lpSum([x[pc] for pc in possible_combinations if pc[0]==stand])
   
    prob+=StandVolumes[stand] == lpSum([vols[pc]*x[pc] for pc in possible_combinations if pc[0]==stand])
print('LP defined, now solving... maxSeconds=', maxSeconds, 'Tolerance=', Tolerance)
#prob.writeLP("test.lp.py")
#prob.writeMPS("test.mps")
#prob.solve()
prob.solve(COIN_CMD(maxSeconds=maxSeconds, fracGap=Tolerance)) # Solve using CBC with logging
npv = prob.variablesDict()['z'].value()
xv = {}; out_vols = {}
numVariables = prob.numVariables(); numConstraints=prob.numConstraints()
print('LP con %d variables y %d restricciones' %(numVariables, numConstraints ))

for k,v in prob.variablesDict().items():
    if k[0]=='x' and v.value()>0.001:
        xv[k] = v.value()
    if k[:13]=='yearly_volume':
        vol_year = int(float(k.split('_')[-1]))
        out_vols[vol_year] = v.value()

ROW_START = 10

for ix, xk in enumerate(sorted(xv.keys())):    # output solution
    row = ix+ROW_START; vk = xv[xk]
    xk=xk.replace("'","|")
    print(xk)
    cur.execute("insert into generic.ex_solucion(datoplano) values('"+xk+"');")
   # print('B: ' ,vk)


year_row = 0
for year, yvol in out_vols.items():    # output volumes
    row = year_row+ROW_START; year_row+=1
    #print('C: ' ,year)
    #print('D: ' ,yvol)
    #print('E: ' ,min_vols[year])
    #print('F: ' ,max_vols[year])
 

conn.commit()

NPV = value(prob.objective)
status = LpStatus[prob.status]
cur.execute("delete from generic.status_pulp where stage = 'public'")
cur.execute("insert into generic.status_pulp(stage, nvp,status,fecha) select 'public',"+str(NPV)+",'"+str(status)+"',CURRENT_TIMESTAMP ;")

conn.commit()

cur.execute("call generic.set_solution()")
conn.commit()
cur.close()
conn.close()
##########################################

#remove(sys.argv[2])
#remove(sys.argv[1])
print('NPV:', npv)  