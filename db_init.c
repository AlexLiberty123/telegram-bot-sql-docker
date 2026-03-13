#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <cjson/cJSON.h>

char* read_file(const char* filename) {
    FILE *f = fopen(filename, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long length = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(length + 1);
    fread(data, 1, length, f);
    data[length] = '\0';
    fclose(f);
    return data;
}

int main() {
    const char *db_host = getenv("DB_HOST") ? getenv("DB_HOST") : "localhost";
    const char *db_user = getenv("DB_USER") ? getenv("DB_USER") : "myuser";
    const char *db_pass = getenv("DB_PASS") ? getenv("DB_PASS") : "mypassword";
    const char *db_name = getenv("DB_NAME") ? getenv("DB_NAME") : "my_video_data";

    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), "host=%s user=%s password=%s dbname=%s", db_host, db_user, db_pass, db_name);

    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "DB Error: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    const char *sql_videos = "CREATE TABLE IF NOT EXISTS videos (id UUID PRIMARY KEY, creator_id TEXT, video_created_at TIMESTAMPTZ, views_count BIGINT, likes_count BIGINT, comments_count BIGINT, reports_count BIGINT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);";
    const char *sql_snapshots = "CREATE TABLE IF NOT EXISTS snapshots (id UUID PRIMARY KEY, video_id UUID REFERENCES videos(id), views_count BIGINT, likes_count BIGINT, comments_count BIGINT, reports_count BIGINT, delta_views_count BIGINT, delta_likes_count BIGINT, delta_comments_count BIGINT, delta_reports_count BIGINT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);";

    PQclear(PQexec(conn, sql_videos));
    PQclear(PQexec(conn, sql_snapshots));

    printf("Читаем videos.json...\n");
    char *json_data = read_file("videos.json");
    if (!json_data) { printf("Файл videos.json не найден!\n"); return 1; }

    cJSON *root = cJSON_Parse(json_data);
    cJSON *videos = cJSON_GetObjectItem(root, "videos");
    cJSON *video;

    cJSON_ArrayForEach(video, videos) {
        const char *v_params[9] = {
            cJSON_GetObjectItem(video, "id")->valuestring,
            cJSON_GetObjectItem(video, "creator_id")->valuestring,
            cJSON_GetObjectItem(video, "video_created_at")->valuestring,
            cJSON_PrintUnformatted(cJSON_GetObjectItem(video, "views_count")),
            cJSON_PrintUnformatted(cJSON_GetObjectItem(video, "likes_count")),
            cJSON_PrintUnformatted(cJSON_GetObjectItem(video, "comments_count")),
            cJSON_PrintUnformatted(cJSON_GetObjectItem(video, "reports_count")),
            cJSON_GetObjectItem(video, "created_at")->valuestring,
            cJSON_GetObjectItem(video, "updated_at")->valuestring
        };

        const char *insert_v = "INSERT INTO videos VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO UPDATE SET views_count=EXCLUDED.views_count, likes_count=EXCLUDED.likes_count, updated_at=EXCLUDED.updated_at;";
        PQclear(PQexecParams(conn, insert_v, 9, NULL, v_params, NULL, NULL, 0));

        for(int i=3; i<=6; i++) free((void*)v_params[i]); // Очищаем память от чисел

        cJSON *snapshots = cJSON_GetObjectItem(video, "snapshots");
        cJSON *snap;
        cJSON_ArrayForEach(snap, snapshots) {
            const char *s_params[12] = {
                cJSON_GetObjectItem(snap, "id")->valuestring,
                cJSON_GetObjectItem(video, "id")->valuestring,
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "views_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "likes_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "comments_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "reports_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "delta_views_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "delta_likes_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "delta_comments_count")),
                cJSON_PrintUnformatted(cJSON_GetObjectItem(snap, "delta_reports_count")),
                cJSON_GetObjectItem(snap, "created_at")->valuestring,
                cJSON_GetObjectItem(snap, "updated_at")->valuestring
            };

            const char *insert_s = "INSERT INTO snapshots VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING;";
            PQclear(PQexecParams(conn, insert_s, 12, NULL, s_params, NULL, NULL, 0));
            for(int i=2; i<=9; i++) free((void*)s_params[i]);
        }
    }

    cJSON_Delete(root);
    free(json_data);
    PQfinish(conn);
    printf("Импорт завершен успешно!\n");
    return 0;
}