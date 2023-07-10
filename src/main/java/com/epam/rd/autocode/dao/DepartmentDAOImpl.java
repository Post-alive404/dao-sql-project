package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDAOImpl implements DepartmentDao{
    private final Connection connection;
    private static final String GET_DEP_BY_ID = "SELECT ID, NAME, LOCATION FROM DEPARTMENT WHERE ID = ?";
    private static final String GET_DEP_ALL = "SELECT ID, NAME, LOCATION FROM DEPARTMENT";
    private static final String INSERT_DEP = "INSERT INTO DEPARTMENT (ID, NAME, LOCATION) VALUES (?, ?, ?) ";
    private static final String UPDATE_DEP = "UPDATE DEPARTMENT SET NAME = ?, LOCATION = ? WHERE ID = ?";
    private static final String DELETE_DEP = "DELETE FROM DEPARTMENT WHERE ID = ?";
    public DepartmentDAOImpl() {
        try {
            connection = ConnectionSource.instance().createConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Department> getById(BigInteger Id) {
        try(PreparedStatement statement = this.connection.prepareStatement(GET_DEP_BY_ID)) {
            statement.setInt(1, Id.intValue());
            ResultSet resultSet = statement.executeQuery();
            Department department;
            while (resultSet.next()){
                BigInteger dep_id = BigInteger.valueOf(resultSet.getInt("ID"));
                String name = resultSet.getString("NAME");
                String location = resultSet.getString("LOCATION");
                department = new Department(dep_id, name, location);
                if (resultSet.next()){
                    continue;
                }
                return Optional.of(department);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public List<Department> getAll() {
        List<Department> departments = new ArrayList<>();
        try(PreparedStatement statement = this.connection.prepareStatement(GET_DEP_ALL)) {
            ResultSet resultSet = statement.executeQuery();
            Department department;
            while (resultSet.next()){
                BigInteger dep_id = BigInteger.valueOf(resultSet.getInt("ID"));
                String name = resultSet.getString("NAME");
                String location = resultSet.getString("LOCATION");
                department = new Department(dep_id, name, location);
                departments.add(department);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return departments;
    }

    @Override
    public Department save(Department department) {
        if (this.getById(department.getId()).orElse(null) == null) {
            try (PreparedStatement statement = this.connection.prepareStatement(INSERT_DEP)) {
                statement.setInt(1, department.getId().intValue());
                statement.setString(2, department.getName());
                statement.setString(3, department.getLocation());
                statement.execute();

                return this.getById(department.getId()).orElse(null);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            try (PreparedStatement statement = this.connection.prepareStatement(UPDATE_DEP)) {
                statement.setString(1, department.getName());
                statement.setString(2, department.getLocation());
                statement.setInt(3, department.getId().intValue());
                statement.execute();

                return this.getById(department.getId()).orElse(null);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void delete(Department department) {
        try (PreparedStatement statement = this.connection.prepareStatement(DELETE_DEP)) {
            statement.setInt(1, department.getId().intValue());

            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
