#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <cjson/cJSON.h>
#include <libpq-fe.h>

#define SYSTEM_PROMPT "You are an intelligent SQL generator for PostgreSQL.\nYour goal: Return a SINGLE NUMBER based on the user's question.\nDATABASE SCHEMA:\n1. `videos` (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count).\n2. `snapshots` (video_id, delta_views_count, delta_likes_count, created_at).\nRULES:\n1. Return ONLY the raw SQL query. No markdown.\n2. If asking for ID/Name/Text -> Return: IGNORE\n3. Combine filters using AND.\n4. Handle numbers with spaces.\n5. \"По итоговой статистике\" -> Use table `videos`.\n6. \"Вышло/Появилось\" (Published) -> Use `video_created_at` from `videos`."

struct MemoryStruct { char *memory; size_t size; };

static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *)userp;
    char *ptr = realloc(mem->memory, mem->size + realsize + 1);
    if(!ptr) return 0;
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    return realsize;
}

char* http_request(const char* url, const char* post_data, struct curl_slist *headers) {
    CURL *curl = curl_easy_init();
    struct MemoryStruct chunk = {malloc(1), 0};
    chunk.memory[0] = '\0';

    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
        if (headers) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        if (post_data) {
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);
        }
        curl_easy_perform(curl);
        curl_easy_cleanup(curl);
    }
    return chunk.memory;
}

char* generate_sql(const char *user_text, const char *api_key) {
    cJSON *req = cJSON_CreateObject();
    cJSON_AddStringToObject(req, "model", "arcee-ai/trinity-large-preview:free");
    cJSON_AddNumberToObject(req, "temperature", 0);
    cJSON *msgs = cJSON_AddArrayToObject(req, "messages");
    
    cJSON *sys = cJSON_CreateObject();
    cJSON_AddStringToObject(sys, "role", "system");
    cJSON_AddStringToObject(sys, "content", SYSTEM_PROMPT);
    cJSON_AddItemToArray(msgs, sys);

    cJSON *usr = cJSON_CreateObject();
    cJSON_AddStringToObject(usr, "role", "user");
    cJSON_AddStringToObject(usr, "content", user_text);
    cJSON_AddItemToArray(msgs, usr);

    char *payload = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    char auth_header[256];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", api_key);
    headers = curl_slist_append(headers, auth_header);

    char *resp = http_request("https://openrouter.ai/api/v1/chat/completions", payload, headers);
    free(payload);
    curl_slist_free_all(headers);

    cJSON *root = cJSON_Parse(resp);
    free(resp);
    if (!root) return NULL;

    cJSON *choices = cJSON_GetObjectItem(root, "choices");
    if (!cJSON_IsArray(choices)) { cJSON_Delete(root); return NULL; }
    
    cJSON *msg = cJSON_GetObjectItem(cJSON_GetArrayItem(choices, 0), "message");
    const char *content = cJSON_GetObjectItem(msg, "content")->valuestring;
    
    if (strstr(content, "IGNORE") != NULL) { cJSON_Delete(root); return NULL; }

    char *clean_sql = strdup(content);
    if (strncmp(clean_sql, "```sql", 6) == 0) memmove(clean_sql, clean_sql + 6, strlen(clean_sql) - 5);
    char *end = strstr(clean_sql, "```");
    if (end) *end = '\0';

    cJSON_Delete(root);
    return clean_sql;
}

char* execute_sql(const char *sql) {
    const char *db_host = getenv("DB_HOST") ? getenv("DB_HOST") : "localhost";
    const char *db_user = getenv("DB_USER") ? getenv("DB_USER") : "myuser";
    const char *db_pass = getenv("DB_PASS") ? getenv("DB_PASS") : "mypassword";
    const char *db_name = getenv("DB_NAME") ? getenv("DB_NAME") : "my_video_data";
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), "host=%s user=%s password=%s dbname=%s", db_host, db_user, db_pass, db_name);

    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) { PQfinish(conn); return NULL; }

    PGresult *res = PQexec(conn, sql);
    char *result_str = NULL;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0 && !PQgetisnull(res, 0, 0)) {
        result_str = strdup(PQgetvalue(res, 0, 0));
    }
    PQclear(res);
    PQfinish(conn);
    return result_str;
}

void send_message(const char *token, long chat_id, const char *text) {
    char url[512];
    snprintf(url, sizeof(url), "https://api.telegram.org/bot%s/sendMessage", token);
    
    cJSON *req = cJSON_CreateObject();
    cJSON_AddNumberToObject(req, "chat_id", chat_id);
    cJSON_AddStringToObject(req, "text", text);
    char *json_str = cJSON_PrintUnformatted(req);
    cJSON_Delete(req);

    struct curl_slist *headers = curl_slist_append(NULL, "Content-Type: application/json");
    char *resp = http_request(url, json_str, headers);
    free(json_str); free(resp); curl_slist_free_all(headers);
}

int main() {
    curl_global_init(CURL_GLOBAL_ALL);
    const char *token = getenv("TG_TOKEN");
    const char *or_key = getenv("OPENROUTER_KEY");
    if (!token || !or_key) { printf("Не найдены TG_TOKEN или OPENROUTER_KEY!\n"); return 1; }

    long update_id = 0;
    printf("C-Bot запущен и слушает Telegram...\n");

    while(1) {
        char url[512];
        snprintf(url, sizeof(url), "https://api.telegram.org/bot%s/getUpdates?offset=%ld&timeout=10", token, update_id);
        char *resp = http_request(url, NULL, NULL);

        if (resp) {
            cJSON *root = cJSON_Parse(resp);
            if (root) {
                cJSON *result = cJSON_GetObjectItem(root, "result");
                cJSON *item;
                cJSON_ArrayForEach(item, result) {
                    update_id = cJSON_GetObjectItem(item, "update_id")->valuedouble + 1;
                    cJSON *msg = cJSON_GetObjectItem(item, "message");
                    if (msg && cJSON_GetObjectItem(msg, "text")) {
                        const char *text = cJSON_GetObjectItem(msg, "text")->valuestring;
                        long chat_id = cJSON_GetObjectItem(cJSON_GetObjectItem(msg, "chat"), "id")->valuedouble;
                        
                        printf("Запрос: %s\n", text);
                        char *sql = generate_sql(text, or_key);
                        if (sql) {
                            printf("Сгенерирован SQL: %s\n", sql);
                            char *db_res = execute_sql(sql);
                            if (db_res) {
                                send_message(token, chat_id, db_res);
                                free(db_res);
                            }
                            free(sql);
                        }
                    }
                }
                cJSON_Delete(root);
            }
            free(resp);
        }
    }
    curl_global_cleanup();
    return 0;
}