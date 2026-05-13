from locust import HttpUser, between, task


class UsuarioHotSale(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(3)  # 30%
    def comprar(self):
        self.client.post("/orders", json={"producto_id": 1, "cantidad": 1})

    @task(7)  # 70%
    def ver_catalogo(self):
        self.client.get("/products")

    @task(1)
    def healthcheck(self):
        self.client.get("/health")
