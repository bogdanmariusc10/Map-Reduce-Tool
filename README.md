Tema 1 APD - Cătănuș Bogdan-Marius, 335CC

Pentru început, am declarat două structuri, MapperArgs și ReducerArgs, pe care
le folosesc pentru argumentele funcțiilor pentru thread-uri.

În MapperArgs am definit o coadă de perechi pentru fișierele de procesat și
id-urile acestora, un vector de perechi pentru a reține cuvintele extrase din
fiecare fișier și indexul fișierului în care se găsesc acestea, un mutex și o
barieră.

În ReducerArgs am definit un vector de perechi pentru cuvintele procesate de
mapperi și indexul fișierului în care se găsesc, o barieră și două caractere
care reprezintă intervalul de litere de care se va ocupa fiecare reducer.

În funcția main preiau argumentele din linia de comandă pe care le stochez în
variabilele mele, deschid fișierul de intrare, parsez numărul de fișiere și
creez un vector pentru a stoca calea acestora. Pentru a mă asigura că fiecare
fișier de procesat se va deschide corect, determin rădăcina acestora și, pe
parcurs ce citesc numele fișierelor, le construiesc calea absolută pe care o
voi stoca în vectorul de fișiere. Construiesc o coadă de perechi în care salvez
numele fișierelor de procesat și id-ul acestora, inițializez vectorul de thread-uri,
creez mutexul și bariera și instanțiez vectoriul în care thread-urile vor salva
cuvintele procesate. În continuare, creez în aceeași buclă for atât thread-urile
mapper, cât și cele reducer, și populez structurile pentru argumentele funcțiilor
aferente. În plus, la crearea reducerilor, calculez intervalul de litere în care
trebuie să lucreze fiecare pentru o bună paralelizare a operației. La final,
aștept ca thread-urile să finalizeze munca și elimin resursele alocate.

În funcția map_operation preiau argumentele funcției și construiesc o buclă while
care parcurge coada de fișiere astfel: folosesc mutex-ul în zona critică de
extragere a fișierului din coadă, deschid fișierul, iar în altă buclă while
citesc cuvintele, elimin caracterele care nu reprezintă litere, formatez toate
cuvintele la minuscule și le adaug în vectorul de cuvinte. La finalul buclei
mari folosesc mutex-ul din nou pentru a adăuga cuvintele în vectorul comun al
thread-urilor mapper. În final, folosesc bariera pentru a aștepta ca toți
mapperii să finalizeze munca.

În funcția reduce_operation preiau argumentele funcției și folosesc bariera pentru
ca reducerii să aștepte toți mapperii să finalizeze munca. În continuare,
creez vectorul de fișiere de ieșire, iar în bucla for creez și deschid fișierele
de ieșire pentru fiecare literă din intervalul de lucru. Apoi populez harta de
cuvinte pe tot intervalul cu cuvintele procesate de mapperi agregate. Într-un
vector de perechi copiez cuvintele din hartă și le sortez folosind o funcție
lambda care compară mai întâi cuvintele după numărul de apariții în diferite
fișiere și apoi lexicografic. La final, scriu elementele din vectorul sortat În
fișierele de ieșire pe care mai apoi le închid.