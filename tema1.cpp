#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <queue>
#include <cctype>
#include <algorithm>
#include <cstdlib>
#include <pthread.h>

using namespace std;

// Structura pentru argumentele functiei map_operation
struct MapperArgs
{
    // Coada pentru fisierele de procesat
    queue<pair<string, int>> *files_queue;
    // Vector pentru cuvintele procesate din fisiere
    vector<pair<string, int>> *words_from_files;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
};


// Structura pentru argumentele functiei reduce_operation
struct ReducerArgs
{
    // Vector pentru cuvintele procesate de mapperi
    vector<pair<string, int>> *words_from_mappers;
    pthread_barrier_t *barrier;
    // Intervalul de lucru pentru fiecare reducer
    char start_char;
    char end_char;
};

// Functia pentru thread-urile mapper
void *map_operation(void *arg)
{
    // Preiau argumentele functiei
    MapperArgs *mapper_args = (MapperArgs*)arg;

    while (1)
    {
        // Asigur accesul unui singur thread la coada de fisiere
        pthread_mutex_lock(mapper_args->mutex);

        // Cand se goleste coada ies din bucla
        if (mapper_args->files_queue->empty())
        {
            pthread_mutex_unlock(mapper_args->mutex);
            break;
        }

        // Pereche <nume fisier, id> si extrag din coada
        pair<string, int> file;
        file = mapper_args->files_queue->front();
        mapper_args->files_queue->pop();

        pthread_mutex_unlock(mapper_args->mutex);

        // Deschid fisierul
        ifstream f(file.first);
        if (!f)
        {
            cerr << "Eroare la deschiderea fisierului: " << file.first << endl;
            return nullptr;
        }

        // Id-urile fisierelor incep de la 1
        int file_id = file.second + 1;

        string word;
        // Formez un vector de perechi <cuvant, id_fisier>
        vector<pair<string, int>> words_from_file;

        while (f >> word)
        {
            // Elimin caracterele care nu sunt litere
            string final_word;
            for (char c : word)
            {
                if (isalpha(c))
                {
                    final_word += c;
                }
            }
            // Transform toate caracterele in minuscule
            transform(final_word.begin(), final_word.end(), final_word.begin(), ::tolower);
            // Adaug perechile pana cand se termina toate cuvintele
            if (!final_word.empty())
            {
                words_from_file.push_back({final_word, file_id});
            }
        }
        f.close();

        // Adaug perechile in structura comuna a mapperilor
        pthread_mutex_lock(mapper_args->mutex);
        mapper_args->words_from_files->insert(mapper_args->words_from_files->end(), words_from_file.begin(), words_from_file.end());
        pthread_mutex_unlock(mapper_args->mutex);
    }

    // Se asteapta ca toti mapperii sa termine lucrul
    pthread_barrier_wait(mapper_args->barrier);

    return nullptr;
}

// Functia pentru thread-urile reducer
void *reduce_operation(void *arg)
{
    // Preiau argumentele functiei
    ReducerArgs* reducer_args = (ReducerArgs*)arg;

    // Se asteapta ca toti mapperii sa termine de procesat cuvintele
    pthread_barrier_wait(reducer_args->barrier);

    // Vector pentru fisierele de iesire ale fiecarui reducer
    int range = reducer_args->end_char - reducer_args->start_char + 1;
    vector<ofstream> out_files(range);

    // Compun numele fisierelor de iesire si le deschid
    for (char c = reducer_args->start_char; c <= reducer_args->end_char; c++)
    {
        string out_file = string(1, c) + ".txt";
        out_files[c - reducer_args->start_char].open(out_file);
    }

    // Construiesc formatul final <cuvant, id-uri fisiere> care va fi scris la iesire
    map<string, set<int>> final_words;
    for (auto &pair : *reducer_args->words_from_mappers)
    {
        // Verific daca prima litera a cuvantului este in intervalul de procesat
        string &word = pair.first;
        if (word[0] >= reducer_args->start_char && word[0] <= reducer_args->end_char)
        {
            // Adaug perechile agregate in structura finala
            final_words[word].insert(pair.second);
        }
    }

    // Copiez datele finale intr-un vector pentru a le sorta conform regulii
    vector<pair<string, set<int>>> sorted_words(final_words.begin(), final_words.end());

    // Sortez cuvintele folosind o functie lambda
    sort(sorted_words.begin(), sorted_words.end(), [](auto &a, auto &b)
    {
        // Mai intai in functie de numarul de fisiere in care apar
        if (a.second.size() != b.second.size())
        {
            return a.second.size() > b.second.size();
        }
        // Apoi in ordine lexicografica
        return a.first < b.first;
    });

    // Scriu datele in fisier folosind formatul cerut
    for (auto &pair : sorted_words)
    {
        // Determin indexul fisierului in care trebuie sa scriu
        string &word = pair.first;
        int file_index = word[0] - reducer_args->start_char;

        out_files[file_index] << word << ":[";

        // Iau un flag pentru a genera spatii doar intre indexurile fisierelor
        bool is_first_index = true;
        for (int ids : pair.second)
        {
            if (!is_first_index)
            {
                out_files[file_index] << " ";
            }
            out_files[file_index] << ids;
            is_first_index = false;
        }
        out_files[file_index] << "]" << endl;
    }

    // Inchid fisierele de iesire
    for (auto &fout : out_files)
    {
        fout.close();
    }

    return nullptr;
}

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        cerr << "Folosire: ./tema1 <numar_mapperi> <numar_reduceri> <fisier_intrare>" << endl;
        return -1;
    }

    // Preiau argumentele din linia de comanda
    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);
    string in_file = argv[3];

    // Deschid fisierul de intrare
    ifstream fin(in_file);
    if (!fin)
    {
        cerr << "Eroare la deschiderea fisierului de intrare." << endl;
        return -1;
    }

    // Citesc numarul de fisiere si numele acestora pe care le stochez intr-un vector
    int num_text_files, i;
    fin >> num_text_files;
    vector<string> text_files(num_text_files);

    // Determin radacina fisierelor
    string root;
    if (in_file.find('/') == string::npos)
    {
        root = ".";
    }
    else
    {
        int last_slash = in_file.find_last_of('/');
        root = in_file.substr(0, last_slash);
    }

    // Construiesc calea absoluta pentru fiecare fisier si o salvez in vector
    for (i = 0; i < num_text_files; i++)
    {
        string text_file;
        fin >> text_file;
        string full_path = root + "/" + text_file;
        char *abs_path = realpath(full_path.c_str(), nullptr);

        if (abs_path != nullptr)
        {
            text_files[i] = string(abs_path);
            free(abs_path);
        }
        else
        {
            cerr << "Nu s-a putut gasi calea: " << full_path << endl;
            return -1;
        }
    }
    fin.close();

    // Creez o coada pentru fisiere in care adaug numele fisierelor si id-ul lor
    queue<pair<string, int>> files_queue;
    for (i = 0; i < num_text_files; i++)
    {
        files_queue.push({text_files[i], i});
    }

    // Initializez vectorul de thread-uri, mutex-ul si bariera
    int num_threads = num_mappers + num_reducers;
    vector<pthread_t> threads(num_threads);
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, num_mappers + num_reducers);

    // Initializez vectorul de cuvinte extrase din fisiere
    vector<pair<string, int>> words_from_files;

    for (i = 0; i < num_mappers + num_reducers; i++)
    {
        // Creez thread-urile mapper
        if (i < num_mappers)
        {
            MapperArgs *mapper_args = new MapperArgs();
            mapper_args->files_queue = &files_queue;
            mapper_args->words_from_files = &words_from_files;
            mapper_args->mutex = &mutex;
            mapper_args->barrier = &barrier;
            pthread_create(&threads[i], nullptr, map_operation, mapper_args);
        }
        // Creez thread-urile reducer
        else
        {
            ReducerArgs *reducer_args = new ReducerArgs();
            reducer_args->words_from_mappers = &words_from_files;
            reducer_args->barrier = &barrier;
            // Calculez intervalul de lucru pentru fiecare reducer
            reducer_args->start_char = 'a' + (i - num_mappers) * 26 / num_reducers;
            reducer_args->end_char = 'a' + (i - num_mappers + 1) * 26 / num_reducers - 1;
            pthread_create(&threads[i], nullptr, reduce_operation, reducer_args);
        }
    }

    // Astept ca thread-urile sa-si termine executia
    for (i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], nullptr);
    }

    // Elimin bariera si mutex-ul
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);

    return 0;
}
