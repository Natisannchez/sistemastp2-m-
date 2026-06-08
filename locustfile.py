from locust import HttpUser, between, task


class UsuarioHotSale(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(8)
    def reservar(self):
        # Endpoint de inventario con lock distribuido.
        self.client.post("/reserve", json={"sku": "SKU-001", "cantidad": 1})

    @task(1)
    def ver_stock(self):
        self.client.get("/stock/SKU-001")

    @task(1)
    def healthcheck(self):
        self.client.get("/health")
